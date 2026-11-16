"""BEP 3/15 trackers, BEP 5 DHT lookup and BEP 9 magnet metadata.

All sockets run on the phone. No remote torrent engine or signing service.
Bounds apply to packet sizes, metadata, peers, DHT queries and total time.
"""

import base64
import concurrent.futures
import hashlib
import ipaddress
import json
import os
import queue
import random
import select
import socket
import struct
import time
import threading
from collections import Counter, OrderedDict
from urllib.parse import parse_qs, quote_from_bytes, urlencode, urlsplit
from urllib.request import Request, urlopen

from .core import Metainfo, TorrentError, _receive, bdecode, bencode

BOOTSTRAP = (('router.bittorrent.com', 6881), ('router.utorrent.com', 6881),
             ('dht.transmissionbt.com', 6881), ('dht.libtorrent.org', 25401),
             ('dht.aelitis.com', 6881))
# Each of these answered two announces of a public-domain torrent on 2026-11-16.
# UDP first; the HTTP entries keep discovery alive on networks that block UDP.
PUBLIC_TRACKERS = ('udp://tracker.opentrackr.org:1337/announce',
                   'udp://open.stealth.si:80/announce',
                   'udp://www.torrent.eu.org:451/announce',
                   'udp://open.demonii.com:1337/announce',
                   'udp://tracker.qu.ax:6969/announce',
                   'udp://open.tracker.cl:1337/announce',
                   'udp://tracker.internetwarriors.net:1337/announce',
                   'udp://tracker-udp.gbitt.info:80/announce',
                   'udp://exodus.desync.com:6969/announce',
                   'udp://retracker01-msk-virt.corbina.net:80/announce',
                   'udp://tracker.dler.org:6969/announce',
                   'http://tracker.opentrackr.org:1337/announce',
                   'http://tracker.dler.org:6969/announce')
CLIENT_PEER_ID = b'-EP0200-' + os.urandom(12)


def peer_id():
    return CLIENT_PEER_ID


def with_public_trackers(trackers):
    """The source's own trackers first, then the built-in public ones."""
    return list(dict.fromkeys(list(trackers) + list(PUBLIC_TRACKERS)))[:32]


# Nodes that answered a recent lookup. Starting from them avoids three DNS
# lookups and the rate limits of the public bootstrap routers on every start.
_dht_nodes = OrderedDict()
_dht_lock = threading.Lock()
_dht_saved = None


def remember_dht_node(address, node_id):
    if not isinstance(node_id, bytes) or len(node_id) != 20:
        return
    with _dht_lock:
        _dht_nodes[address] = node_id
        _dht_nodes.move_to_end(address)
        while len(_dht_nodes) > 64:
            _dht_nodes.popitem(last=False)


def load_dht_nodes(path):
    try:
        with open(path, encoding='utf-8') as handle:
            rows = json.loads(handle.read(65537))
        for row in rows[:64]:
            host, port = validate_address(row[0], row[1])
            remember_dht_node((host, port), bytes.fromhex(row[2]))
    except (OSError, ValueError, TypeError, IndexError, KeyError, TorrentError):
        pass


def save_dht_nodes(path):
    global _dht_saved
    with _dht_lock:
        rows = [[host, port, node_id.hex()] for (host, port), node_id in _dht_nodes.items()]
    if not rows or rows == _dht_saved:
        return
    try:
        temporary = path + '.tmp'
        with open(temporary, 'w', encoding='utf-8') as handle:
            json.dump(rows, handle, separators=(',', ':'))
        os.replace(temporary, path)
        _dht_saved = rows
    except OSError:
        pass  # A lookup cache must never prevent playback.


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
    reply = bdecode(payload, strict=False)
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
            except (KeyError, ValueError, AttributeError, TypeError, TorrentError):
                pass
    if isinstance(reply.get(b'peers6'), bytes):
        result.extend(compact_peers(reply[b'peers6'], ipv6=True, allow_local=allow_local))
    return list(dict.fromkeys(result))[:200]


def udp_tracker(url, info_hash, identity, left=0, timeout=6, allow_local=False):
    parsed = urlsplit(url)
    if parsed.scheme != 'udp' or not parsed.hostname or parsed.username:
        raise TorrentError('Invalid UDP tracker')
    # IPv4 first. An IPv6-only mobile network (NAT64/DNS64) has no IPv4 route,
    # so the connect fails at once and the synthesized IPv6 address is used.
    endpoints = sorted(socket.getaddrinfo(parsed.hostname, parsed.port or 80, 0, socket.SOCK_DGRAM),
                       key=lambda entry: entry[0] != socket.AF_INET)
    deadline = time.monotonic() + timeout
    channel = None
    for family, _, _, _, address in endpoints[:4]:
        try:
            candidate = socket.socket(family, socket.SOCK_DGRAM)
        except OSError:
            continue
        try:
            candidate.connect(address)
            channel = candidate
            break
        except OSError:
            candidate.close()
    if channel is None:
        raise TorrentError('UDP tracker is unreachable')
    with channel:

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
        payload = response[20:]
        if channel.family == socket.AF_INET:
            return compact_peers(payload, allow_local=allow_local)
        # Over IPv6 a tracker sends 18-byte IPv6 entries, or 6-byte IPv4 entries
        # when it saw a NAT64 gateway. Reading an IPv6 list as IPv4 would invent
        # three endpoints per peer, so the list is IPv6 only when every entry is a
        # valid public IPv6 endpoint; a single such entry is too little to be sure.
        entries = len(payload) // 18 if payload and len(payload) % 18 == 0 else 0
        native = []
        # Every address must lie in 2000::/3, the only global unicast space in use;
        # the first octets of a list of IPv4 addresses almost never all do.
        if entries and (allow_local or all(payload[offset] & 0xE0 == 0x20 for offset in range(0, len(payload), 18))):
            try:
                native = compact_peers(payload, ipv6=True, allow_local=allow_local)
            except TorrentError:
                native = []
            if len(native) < len({payload[offset:offset + 18] for offset in range(0, len(payload), 18)}):
                native = []
        if native and entries > 1:
            return native
        try:
            mapped = compact_peers(payload, allow_local=allow_local)
        except TorrentError:
            mapped = []
        return list(dict.fromkeys(native + mapped))


def end_executor(executor):
    """Shut an executor down without waiting, so that every idle thread really leaves.

    Idle threads wake each other by passing one sentinel along. A thread that
    Kodi is ending by force dies without passing it on, and the others would
    wait for ever; here each of them gets a sentinel of its own.
    """
    executor.shutdown(wait=False)
    try:
        for _ in range(len(executor._threads)):
            executor._work_queue.put(None)
    except Exception:
        pass   # Another executor implementation: the plain shutdown has to do.


def tracker_peers(trackers, info_hash, left=0, timeout=10, allow_local=False, cancel=None,
                  enough=None, found=None):
    """Announce to every tracker in parallel.

    ``found`` receives each tracker's peers the moment it answers, and
    ``enough`` returns early: one dead tracker no longer delays all the others.
    """
    identity = peer_id()
    result = []
    errors = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
    futures = []
    deadline = time.monotonic() + timeout
    try:
        for url in list(dict.fromkeys(trackers))[:24]:
            method = udp_tracker if urlsplit(url).scheme == 'udp' else http_tracker
            futures.append(executor.submit(method, url, info_hash, identity, left, min(timeout, 6), allow_local))
        pending = set(futures)
        while pending and time.monotonic() < deadline:
            done, pending = concurrent.futures.wait(pending, timeout=min(.25, max(0, deadline - time.monotonic())),
                                                    return_when=concurrent.futures.FIRST_COMPLETED)
            if cancel is not None and cancel.is_set():
                raise TorrentError('Cancelled')
            for future in done:
                try:
                    peers = future.result()
                except Exception as error:
                    # A tracker reply is untrusted input (and plain HTTP can be altered
                    # on the way): whatever it breaks, the other trackers still count.
                    errors.append(type(error).__name__)
                    continue
                result.extend(peers)
                if found is not None and peers:
                    found(peers)
            if enough and len(set(result)) >= enough:
                break
        return list(dict.fromkeys(result))[:200], errors
    finally:
        for future in futures:
            future.cancel()
        end_executor(executor)


def dht_peers(info_hash, timeout=12, bootstrap=BOOTSTRAP, allow_local=False, cancel=None, found=None,
              memory=None):
    """Bounded iterative IPv4 KRPC get_peers lookup; not an inbound DHT node.

    ``memory`` starts from nodes that answered earlier lookups and remembers the
    ones that answer now; by default only for the real public bootstrap routers.
    """
    node_id = os.urandom(20)
    deadline = time.monotonic() + timeout
    target = int.from_bytes(info_hash, 'big')
    queued = []
    routers = list(bootstrap[:6])
    remember = (bootstrap is BOOTSTRAP and not allow_local) if memory is None else memory
    if remember:
        with _dht_lock:
            queued = [(int.from_bytes(known_id, 'big') ^ target, address)
                      for address, known_id in _dht_nodes.items()]

    def resolve_routers():
        while routers:
            host, port = routers.pop(0)
            try:
                queued.append((0, socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_DGRAM)[0][4]))
            except OSError:
                continue

    if len(queued) < 8:
        resolve_routers()
    seen = set()
    pending = {}
    result = []
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as channel:
        channel.setblocking(False)
        while time.monotonic() < deadline and len(seen) < 128:
            if cancel is not None and cancel.is_set():
                raise TorrentError('Cancelled')
            now = time.monotonic()
            pending = {key: item for key, item in pending.items() if now - item[1] < 1.5}
            if not queued and not pending:
                resolve_routers()   # Remembered nodes went stale: fall back to the routers.
            queued.sort(key=lambda item: item[0])
            while queued and len(pending) < 8:
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
                reply = bdecode(packet, strict=False)
                transaction = reply.get(b't')
                if transaction not in pending or pending[transaction][0] != sender:
                    continue
                del pending[transaction]
                response = reply.get(b'r')
                if reply.get(b'y') != b'r' or not isinstance(response, dict):
                    continue
                if remember:
                    remember_dht_node(sender, response.get(b'id'))
                fresh = []
                for value in response.get(b'values', [])[:200]:
                    fresh.extend(compact_peers(value, allow_local=allow_local))
                result.extend(fresh)
                if found is not None and fresh:
                    found(fresh)
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
            if len(set(result)) >= 80:
                break
    return list(dict.fromkeys(result))[:200]


def read_message(channel, deadline):
    length = struct.unpack('!I', _receive(channel, 4, deadline))[0]
    if length > 256 * 1024:
        raise TorrentError('Peer message too large')
    return _receive(channel, length, deadline) if length else b''


def send_message(channel, payload):
    channel.sendall(struct.pack('!I', len(payload)) + payload)


def handshake(channel, info_hash, deadline, extended=True, fast=False):
    reserved = bytearray(8)
    if extended:
        reserved[5] = 0x10
    if fast:
        reserved[7] |= 0x04  # BEP 6: explicit rejects, HAVE_ALL/HAVE_NONE, allowed-fast.
    packet = b'\x13BitTorrent protocol' + bytes(reserved) + info_hash + peer_id()
    channel.sendall(packet)
    response = _receive(channel, 68, deadline)
    if response[:20] != packet[:20] or response[28:48] != info_hash:
        raise TorrentError('Peer handshake mismatch')
    return response[20:28]


def fetch_metadata(info_hash, address, trackers=(), timeout=10):
    deadline = time.monotonic() + timeout
    with socket.create_connection(address, timeout=min(3, timeout)) as channel:
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
            header = bdecode(message[2:], strict=False)
            if not isinstance(header, dict) or not isinstance(header.get(b'm', {}), dict):
                raise TorrentError('Invalid metadata extension handshake')
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
            header, end = bdecode(message[2:], prefix=True, strict=False)
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
                   use_dht=True, bootstrap=BOOTSTRAP, diagnostics=None, known=None):
    """Verified metadata and candidate peers for a magnet.

    ``known`` is metadata verified in an earlier session. It is returned as it
    is, and only peers are looked for: about forty, or eight seconds.
    """
    parsed = parse_magnet(uri)
    if cancel is not None and cancel.is_set():
        raise TorrentError('Cancelled')
    info_hash = parsed['info_hash']
    # A magnet does not say whether its torrent is private; DHT is already
    # used here, so the built-in public trackers reveal nothing more.
    trackers = with_public_trackers(parsed['trackers'])
    deadline = time.monotonic() + timeout
    peers, scheduled = [], set()
    arrivals = queue.Queue()
    errors = Counter()
    stopped = threading.Event()

    class Cancellation:
        def is_set(self):
            return stopped.is_set() or bool(cancel is not None and cancel.is_set())

    combined_cancel = Cancellation()
    discovery = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    # Most announced addresses are stale; the first live peer answers within a
    # second, so many are tried together instead of twelve at a time.
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=32)
    futures = {}

    def metadata(address):
        remaining = deadline - time.monotonic()
        if remaining <= 0 or combined_cancel.is_set():
            raise TorrentError('Cancelled')
        return fetch_metadata(info_hash, address, trackers, min(8, remaining)), address

    if known is not None and known.info_hash != info_hash:
        known = None
    if known is not None:
        known.trackers = trackers
        deadline = time.monotonic() + min(timeout, 8)

    def schedule(addresses):
        candidates = list(dict.fromkeys(addresses))
        random.SystemRandom().shuffle(candidates)
        for address in candidates:
            if address not in peers and len(peers) < 200:
                peers.append(address)
            if known is None and address not in scheduled and len(scheduled) < 160:
                scheduled.add(address)
                futures[executor.submit(metadata, address)] = 'metadata'

    try:
        schedule(explicit_peers)
        # Peers are tried for metadata the moment one tracker or DHT node names
        # them, not after the slowest tracker has timed out.
        futures[discovery.submit(tracker_peers, trackers, info_hash, timeout=min(8, timeout),
                                 allow_local=allow_local, cancel=combined_cancel, found=arrivals.put)] = 'tracker'
        if use_dht:
            futures[discovery.submit(dht_peers, info_hash, timeout=min(12, timeout), bootstrap=bootstrap,
                                     allow_local=allow_local, cancel=combined_cancel, found=arrivals.put)] = 'dht'
        while futures and time.monotonic() < deadline:
            if cancel is not None and cancel.is_set():
                raise TorrentError('Cancelled')
            done, _ = concurrent.futures.wait(futures, timeout=0.2,
                                                   return_when=concurrent.futures.FIRST_COMPLETED)
            while True:
                try:
                    schedule(arrivals.get_nowait())
                except queue.Empty:
                    break
            if known is not None and len(peers) >= 40:
                break
            for future in done:
                kind = futures.pop(future)
                try:
                    result = future.result()
                    if kind == 'metadata':
                        meta, successful_peer = result
                        meta.metadata_peers = (successful_peer,)
                        if diagnostics is not None:
                            diagnostics.update({'candidate_peers': len(peers), 'metadata_verified': True,
                                                'responsive_peer_prioritized': True})
                        return meta, [successful_peer] + [peer for peer in peers if peer != successful_peer]
                    if kind == 'tracker':
                        result, tracker_errors = result
                        errors.update(tracker_errors)
                    schedule(result)
                except Exception as error:
                    # One malformed reply from a peer, tracker or DHT node is never
                    # a reason to give up on the others.
                    errors[type(error).__name__ + ': ' + str(error)[:100]] += 1
        if known is not None and peers:
            if diagnostics is not None:
                diagnostics.update({'candidate_peers': len(peers), 'metadata_verified': True, 'saved_metadata': True})
            return known, peers
        if diagnostics is not None:
            diagnostics.update({'candidate_peers': len(peers), 'metadata_verified': False, 'errors': dict(errors)})
        if not peers:
            raise TorrentError('No torrent peers found from trackers or DHT; try another source')
        raise TorrentError('Found {} peers but none supplied usable torrent metadata; try another source'.format(len(peers)))
    finally:
        stopped.set()
        for future in futures:
            future.cancel()
        end_executor(executor)
        end_executor(discovery)
