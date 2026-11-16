"""BEP 3/15 trackers, BEP 5 DHT lookup and BEP 9 magnet metadata.

All sockets run on the phone. No remote torrent engine or signing service.
Bounds apply to packet sizes, metadata, peers, DHT queries and total time.
"""

import base64
import concurrent.futures
import hashlib
import ipaddress
import os
import random
import select
import socket
import struct
import time
from urllib.parse import parse_qs, quote_from_bytes, urlencode, urlsplit
from urllib.request import Request, urlopen

from .core import Metainfo, TorrentError, _receive, bdecode, bencode

BOOTSTRAP = (('router.bittorrent.com', 6881), ('router.utorrent.com', 6881),
             ('dht.transmissionbt.com', 6881))
PUBLIC_TRACKERS = ('udp://tracker.opentrackr.org:1337/announce',
                   'udp://open.stealth.si:80/announce')
CLIENT_PEER_ID = b'-EP0200-' + os.urandom(12)


def peer_id():
    return CLIENT_PEER_ID


def validate_address(host, port, allow_local=False):
    if not isinstance(host, str) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise TorrentError('Invalid peer address')
    try:
        ip = ipaddress.ip_address(host)
    except ValueError as error:
        raise TorrentError('Peer address must be numeric') from error
    if ip.is_unspecified or ip.is_multicast or (not allow_local and not ip.is_global):
        raise TorrentError('Nonpublic peer address rejected')
    return str(ip), port


def compact_peers(data, ipv6=False, allow_local=False):
    width = 18 if ipv6 else 6
    if not isinstance(data, bytes) or len(data) % width or len(data) > 65520:
        raise TorrentError('Malformed compact peer list')
    result = []
    for offset in range(0, len(data), width):
        host = socket.inet_ntop(socket.AF_INET6 if ipv6 else socket.AF_INET, data[offset:offset + width - 2])
        port = struct.unpack_from('!H', data, offset + width - 2)[0]
        try:
            address = validate_address(host, port, allow_local)
        except TorrentError:
            continue
        if address not in result:
            result.append(address)
    return result[:200]


def parse_magnet(uri):
    if not isinstance(uri, str) or len(uri) > 65536 or not uri.startswith('magnet:?'):
        raise TorrentError('Invalid magnet URI')
    values = parse_qs(urlsplit(uri).query, max_num_fields=128)
    hashes = []
    for xt in values.get('xt', []):
        if not xt.lower().startswith('urn:btih:'):
            continue
        value = xt[9:]
        try:
            digest = bytes.fromhex(value) if len(value) == 40 else base64.b32decode(value.upper()) if len(value) == 32 else b''
        except (ValueError, base64.binascii.Error):
            raise TorrentError('Invalid BitTorrent v1 magnet hash')
        if len(digest) != 20:
            raise TorrentError('Invalid BitTorrent v1 magnet hash')
        hashes.append(digest)
    if not hashes or len(set(hashes)) != 1:
        raise TorrentError('A magnet must contain one unambiguous v1 hash')
    trackers = []
    for url in values.get('tr', [])[:32]:
        parsed = urlsplit(url)
        if parsed.scheme in ('http', 'https', 'udp') and parsed.hostname and not parsed.username and url not in trackers:
            trackers.append(url)
    return {'info_hash': hashes[0], 'trackers': trackers, 'name': values.get('dn', [''])[0]}


def http_tracker(url, info_hash, identity, left=0, timeout=8, allow_local=False):
    params = 'info_hash=' + quote_from_bytes(info_hash, safe='')
    params += '&peer_id=' + quote_from_bytes(identity, safe='')
    params += '&' + urlencode({'port': 6881, 'uploaded': 0, 'downloaded': 0, 'left': max(1, left),
                               'compact': 1, 'numwant': 80, 'event': 'started'})
    request = Request(url + ('&' if '?' in url else '?') + params,
                      headers={'User-Agent': 'ElementumPython/0.2'})
    with urlopen(request, timeout=timeout) as response:
        payload = response.read(1024 * 1024 + 1)
    if len(payload) > 1024 * 1024:
        raise TorrentError('Tracker response too large')
    reply = bdecode(payload)
    if not isinstance(reply, dict) or b'failure reason' in reply:
        raise TorrentError('Tracker rejected the announce')
    peers = reply.get(b'peers', b'')
    result = []
    if isinstance(peers, bytes):
        result.extend(compact_peers(peers, allow_local=allow_local))
    elif isinstance(peers, list):
        for entry in peers[:200]:
            try:
                result.append(validate_address(entry[b'ip'].decode('ascii'), entry[b'port'], allow_local))
            except (KeyError, ValueError, AttributeError, TorrentError):
                pass
    if isinstance(reply.get(b'peers6'), bytes):
        result.extend(compact_peers(reply[b'peers6'], ipv6=True, allow_local=allow_local))
    return list(dict.fromkeys(result))[:200]


def udp_tracker(url, info_hash, identity, left=0, timeout=6, allow_local=False):
    parsed = urlsplit(url)
    if parsed.scheme != 'udp' or not parsed.hostname or parsed.username:
        raise TorrentError('Invalid UDP tracker')
    address = socket.getaddrinfo(parsed.hostname, parsed.port or 80, socket.AF_INET, socket.SOCK_DGRAM)[0][4]
    deadline = time.monotonic() + timeout
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as channel:
        channel.connect(address)

        def exchange(packet, action, transaction):
            channel.send(packet)
            while True:
                channel.settimeout(max(0.01, deadline - time.monotonic()))
                response = channel.recv(65536)
                if len(response) < 8:
                    continue
                returned_action, returned_id = struct.unpack_from('!II', response)
                if returned_id != transaction:
                    continue
                if returned_action == 3:
                    raise TorrentError('UDP tracker rejected request')
                if returned_action != action:
                    raise TorrentError('Unexpected tracker action')
                return response

        transaction = int.from_bytes(os.urandom(4), 'big')
        response = exchange(struct.pack('!QII', 0x41727101980, 0, transaction), 0, transaction)
        if len(response) != 16:
            raise TorrentError('Invalid tracker connection response')
        connection = struct.unpack_from('!Q', response, 8)[0]
        transaction = int.from_bytes(os.urandom(4), 'big')
        packet = struct.pack('!QII20s20sQQQIIIiH', connection, 1, transaction, info_hash, identity,
                             0, max(1, left), 0, 2, 0, int.from_bytes(os.urandom(4), 'big'), 80, 6881)
        response = exchange(packet, 1, transaction)
        if len(response) < 20:
            raise TorrentError('Short UDP announce response')
        return compact_peers(response[20:], allow_local=allow_local)


def tracker_peers(trackers, info_hash, left=0, timeout=10, allow_local=False, cancel=None):
    identity = peer_id()
    result = []
    errors = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    futures = []
    try:
        for url in list(dict.fromkeys(trackers))[:16]:
            method = udp_tracker if urlsplit(url).scheme == 'udp' else http_tracker
            futures.append(executor.submit(method, url, info_hash, identity, left, min(timeout, 6), allow_local))
        done, _ = concurrent.futures.wait(futures, timeout=timeout)
        for future in done:
            if cancel is not None and cancel.is_set():
                raise TorrentError('Cancelled')
            try:
                result.extend(future.result())
            except (OSError, TorrentError, ValueError) as error:
                errors.append(type(error).__name__)
        return list(dict.fromkeys(result))[:200], errors
    finally:
        for future in futures:
            future.cancel()
        executor.shutdown(wait=False)


def dht_peers(info_hash, timeout=12, bootstrap=BOOTSTRAP, allow_local=False, cancel=None):
    """Bounded iterative IPv4 KRPC get_peers lookup; not an inbound DHT node."""
    node_id = os.urandom(20)
    deadline = time.monotonic() + timeout
    queued = []
    for host, port in bootstrap[:6]:
        try:
            queued.append((0, socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_DGRAM)[0][4]))
        except OSError:
            continue
    seen = set()
    pending = {}
    result = []
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as channel:
        channel.setblocking(False)
        while time.monotonic() < deadline and len(seen) < 64:
            if cancel is not None and cancel.is_set():
                raise TorrentError('Cancelled')
            now = time.monotonic()
            pending = {key: item for key, item in pending.items() if now - item[1] < 1.5}
            queued.sort(key=lambda item: item[0])
            while queued and len(pending) < 4:
                _, address = queued.pop(0)
                if address in seen:
                    continue
                seen.add(address)
                transaction = os.urandom(4)
                payload = bencode({b't': transaction, b'y': b'q', b'q': b'get_peers',
                                   b'a': {b'id': node_id, b'info_hash': info_hash, b'want': [b'n4']}})
                try:
                    channel.sendto(payload, address)
                    pending[transaction] = (address, now)
                except OSError:
                    continue
            if not pending:
                break
            readable, _, _ = select.select([channel], [], [], min(0.2, max(0, deadline - time.monotonic())))
            if not readable:
                continue
            packet, sender = channel.recvfrom(65536)
            try:
                reply = bdecode(packet)
                transaction = reply.get(b't')
                if transaction not in pending or pending[transaction][0] != sender:
                    continue
                del pending[transaction]
                response = reply.get(b'r')
                if reply.get(b'y') != b'r' or not isinstance(response, dict):
                    continue
                for value in response.get(b'values', [])[:200]:
                    result.extend(compact_peers(value, allow_local=allow_local))
                nodes = response.get(b'nodes', b'')
                if not isinstance(nodes, bytes) or len(nodes) % 26 or len(nodes) > 26 * 200:
                    continue
                for offset in range(0, len(nodes), 26):
                    candidate_id = nodes[offset:offset + 20]
                    host = socket.inet_ntoa(nodes[offset + 20:offset + 24])
                    port = struct.unpack_from('!H', nodes, offset + 24)[0]
                    try:
                        address = validate_address(host, port, allow_local)
                    except TorrentError:
                        continue
                    if address not in seen and len(queued) < 512:
                        distance = int.from_bytes(candidate_id, 'big') ^ int.from_bytes(info_hash, 'big')
                        queued.append((distance, address))
            except (TorrentError, ValueError, TypeError, AttributeError):
                continue
            if len(set(result)) >= 40:
                break
    return list(dict.fromkeys(result))[:200]


def read_message(channel, deadline):
    length = struct.unpack('!I', _receive(channel, 4, deadline))[0]
    if length > 256 * 1024:
        raise TorrentError('Peer message too large')
    return _receive(channel, length, deadline) if length else b''


def send_message(channel, payload):
    channel.sendall(struct.pack('!I', len(payload)) + payload)


def handshake(channel, info_hash, deadline, extended=True):
    reserved = bytearray(8)
    if extended:
        reserved[5] = 0x10
    packet = b'\x13BitTorrent protocol' + bytes(reserved) + info_hash + peer_id()
    channel.sendall(packet)
    response = _receive(channel, 68, deadline)
    if response[:20] != packet[:20] or response[28:48] != info_hash:
        raise TorrentError('Peer handshake mismatch')
    return response[20:28]


def fetch_metadata(info_hash, address, trackers=(), timeout=10):
    deadline = time.monotonic() + timeout
    with socket.create_connection(address, timeout=min(4, timeout)) as channel:
        reserved = handshake(channel, info_hash, deadline)
        if not reserved[5] & 0x10:
            raise TorrentError('Peer does not support metadata exchange')
        send_message(channel, b'\x14\x00' + bencode({b'm': {b'ut_metadata': 1}, b'v': b'ElementumPython/0.2'}))
        metadata_size = None
        extension_id = None
        while metadata_size is None:
            message = read_message(channel, deadline)
            if len(message) < 3 or message[:2] != b'\x14\x00':
                continue
            header = bdecode(message[2:])
            metadata_size = header.get(b'metadata_size')
            extension_id = header.get(b'm', {}).get(b'ut_metadata')
            if (type(metadata_size) is not int or not 1 <= metadata_size <= 2 * 1024 * 1024 or
                    type(extension_id) is not int or not 1 <= extension_id <= 255):
                raise TorrentError('Invalid metadata extension handshake')
        count = (metadata_size + 16383) // 16384
        pending = set()
        pieces = {}
        while len(pieces) < count:
            for index in range(count):
                if len(pending) >= 4:
                    break
                if index not in pieces and index not in pending:
                    send_message(channel, bytes((20, extension_id)) + bencode({b'msg_type': 0, b'piece': index}))
                    pending.add(index)
            message = read_message(channel, deadline)
            if len(message) < 3 or message[:2] != b'\x14\x01':
                continue
            header, end = bdecode(message[2:], prefix=True)
            if not isinstance(header, dict):
                raise TorrentError('Malformed metadata response')
            kind, index = header.get(b'msg_type'), header.get(b'piece')
            if kind == 2:
                raise TorrentError('Peer rejected metadata request')
            if kind != 1:
                continue
            if type(index) is not int or index not in pending or header.get(b'total_size') != metadata_size:
                raise TorrentError('Unrequested metadata block')
            content = message[2 + end:]
            if len(content) != min(16384, metadata_size - index * 16384):
                raise TorrentError('Wrong metadata block size')
            pieces[index] = content
            pending.remove(index)
        info = b''.join(pieces[index] for index in range(count))
        if hashlib.sha1(info).digest() != info_hash:
            raise TorrentError('Metadata info-hash verification failed')
        decoded = bdecode(info)
        envelope = {b'info': decoded}
        if trackers:
            envelope[b'announce'] = trackers[0].encode('utf-8')
            envelope[b'announce-list'] = [[url.encode('utf-8')] for url in trackers]
        return Metainfo(bencode(envelope))


def resolve_magnet(uri, timeout=45, cancel=None, explicit_peers=(), allow_local=False,
                   use_dht=True, bootstrap=BOOTSTRAP):
    parsed = parse_magnet(uri)
    info_hash = parsed['info_hash']
    trackers = parsed['trackers'] or list(PUBLIC_TRACKERS)
    deadline = time.monotonic() + timeout
    peers = list(explicit_peers)
    found, _ = tracker_peers(trackers, info_hash, timeout=min(8, timeout), allow_local=allow_local, cancel=cancel)
    peers.extend(found)
    if use_dht and len(peers) < 4 and time.monotonic() < deadline:
        peers.extend(dht_peers(info_hash, timeout=min(10, deadline - time.monotonic()),
                               bootstrap=bootstrap, allow_local=allow_local, cancel=cancel))
    peers = list(dict.fromkeys(peers))[:200]
    random.SystemRandom().shuffle(peers)
    peers = peers[:80]
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=6)
    futures = []
    try:
        for address in peers:
            if cancel is not None and cancel.is_set():
                raise TorrentError('Cancelled')
            futures.append(executor.submit(fetch_metadata, info_hash, address, trackers,
                                           min(8, max(0.1, deadline - time.monotonic()))))
        while futures and time.monotonic() < deadline:
            if cancel is not None and cancel.is_set():
                raise TorrentError('Cancelled')
            done, pending = concurrent.futures.wait(futures, timeout=0.2,
                                                   return_when=concurrent.futures.FIRST_COMPLETED)
            futures = list(pending)
            for future in done:
                try:
                    meta = future.result()
                    return meta, peers
                except (OSError, TorrentError, ValueError):
                    continue
        raise TorrentError('No peer supplied verified magnet metadata before the deadline')
    finally:
        for future in futures:
            future.cancel()
        executor.shutdown(wait=False)
