import base64
import hashlib
import json
from pathlib import Path
import socket
import socketserver
import struct
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote, parse_qs, urlsplit
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'addon/resources/lib'))
from elementum_py.core import Metainfo, PieceStore, TorrentError, _receive, bdecode, bencode
from elementum_py.discovery import (compact_peers, dht_peers, fetch_metadata, http_tracker,
                                    parse_magnet, peer_id, read_message, resolve_magnet,
                                    send_message, tracker_peers, udp_tracker)
from elementum_py.peers import PeerPool, PeerSession
from elementum_py.streaming import StreamServer


class Fixture:
    def __init__(self):
        self.payload = bytes(range(251)) * 3307
        self.piece_length = 32768
        self.info = {b'name': b'fixture.mp4', b'length': len(self.payload),
                     b'piece length': self.piece_length, b'source': b'fixture' * 3000,
                     b'pieces': b''.join(hashlib.sha1(self.payload[offset:offset + self.piece_length]).digest()
                                         for offset in range(0, len(self.payload), self.piece_length))}
        self.meta = Metainfo(bencode({b'info': self.info}))
        self.connections = 0
        self.corrupt_piece = False
        self.corrupt_metadata = False
        self.wrong_bitfield = False
        self.lock = threading.Lock()
        self.servers = []
        owner = self

        class TCP(socketserver.BaseRequestHandler):
            def handle(self):
                with owner.lock:
                    owner.connections += 1
                try:
                    deadline = time.monotonic() + 12
                    handshake = _receive(self.request, 68, deadline)
                    if handshake[28:48] != owner.meta.info_hash:
                        return
                    reserved = bytearray(8)
                    reserved[5] = 0x10
                    self.request.sendall(handshake[:20] + bytes(reserved) + owner.meta.info_hash + b'-FIX000-' + bytes(12))
                    bits = bytearray((len(owner.meta.hashes) + 7) // 8)
                    for index in range(len(owner.meta.hashes)):
                        bits[index // 8] |= 128 >> (index % 8)
                    if owner.wrong_bitfield:
                        bits.append(0)
                    send_message(self.request, b'\x05' + bytes(bits))
                    send_message(self.request, b'\x14\x00' + bencode({b'm': {b'ut_metadata': 7},
                                                                    b'metadata_size': len(owner.meta.info_bytes)}))
                    send_message(self.request, b'\x01')
                    incoming_metadata_id = 1
                    while True:
                        message = read_message(self.request, deadline)
                        if not message:
                            continue
                        if message[:2] == b'\x14\x00':
                            incoming_metadata_id = bdecode(message[2:]).get(b'm', {}).get(b'ut_metadata', 1)
                        elif message[:2] == b'\x14\x07':
                            header = bdecode(message[2:])
                            if header.get(b'msg_type') != 0:
                                continue
                            index = header[b'piece']
                            data = owner.meta.info_bytes[index * 16384:(index + 1) * 16384]
                            if owner.corrupt_metadata and data:
                                data = bytes([data[0] ^ 1]) + data[1:]
                            reply = bencode({b'msg_type': 1, b'piece': index,
                                             b'total_size': len(owner.meta.info_bytes)}) + data
                            send_message(self.request, bytes((20, incoming_metadata_id)) + reply)
                        elif message[0] == 6:
                            index, offset, length = struct.unpack('!III', message[1:])
                            data = owner.payload[index * owner.piece_length + offset:index * owner.piece_length + offset + length]
                            if owner.corrupt_piece and data:
                                data = bytes([data[0] ^ 1]) + data[1:]
                            reply = struct.pack('!BII', 7, index, offset) + data
                            frame = struct.pack('!I', len(reply)) + reply
                            self.request.sendall(frame[:6])
                            self.request.sendall(frame[6:])
                except (OSError, TorrentError, ValueError):
                    return

        class Threaded(socketserver.ThreadingTCPServer):
            daemon_threads = True
            allow_reuse_address = True

        self.tcp = Threaded(('127.0.0.1', 0), TCP)
        self.start(self.tcp)
        self.compact = socket.inet_aton('127.0.0.1') + struct.pack('!H', self.tcp.server_address[1])

        class HTTP(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                query = parse_qs(urlsplit(self.path).query, encoding='latin-1')
                if query.get('compact') != ['1']:
                    self.send_error(400)
                    return
                payload = bencode({b'interval': 1800, b'peers': owner.compact})
                self.send_response(200)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self.http = ThreadingHTTPServer(('127.0.0.1', 0), HTTP)
        self.start(self.http)
        self.http_url = 'http://127.0.0.1:{}/announce'.format(self.http.server_port)

        class UDP(socketserver.BaseRequestHandler):
            def handle(self):
                packet, channel = self.request
                action, transaction = struct.unpack_from('!II', packet, 8)
                if action == 0:
                    reply = struct.pack('!IIQ', 0, transaction, 1234567)
                else:
                    if len(packet) != 98:
                        return
                    reply = struct.pack('!IIIII', 1, transaction, 1800, 1, 1) + owner.compact
                bad = reply[:4] + struct.pack('!I', transaction ^ 1) + reply[8:]
                channel.sendto(bad, self.client_address)
                channel.sendto(reply, self.client_address)

        self.udp = socketserver.UDPServer(('127.0.0.1', 0), UDP)
        self.start(self.udp)
        self.udp_url = 'udp://127.0.0.1:{}/announce'.format(self.udp.server_address[1])

        class DHTLeaf(socketserver.BaseRequestHandler):
            def handle(self):
                packet, channel = self.request
                query = bdecode(packet)
                reply = {b't': query[b't'], b'y': b'r',
                         b'r': {b'id': bytes([2]) * 20, b'token': b'token', b'values': [owner.compact]}}
                channel.sendto(bencode(reply), self.client_address)

        self.dht_leaf = socketserver.UDPServer(('127.0.0.1', 0), DHTLeaf)
        self.start(self.dht_leaf)

        class DHTRoot(socketserver.BaseRequestHandler):
            def handle(self):
                packet, channel = self.request
                query = bdecode(packet)
                node = bytes([2]) * 20 + socket.inet_aton('127.0.0.1') + struct.pack('!H', owner.dht_leaf.server_address[1])
                reply = {b't': query[b't'], b'y': b'r',
                         b'r': {b'id': bytes([1]) * 20, b'token': b'token', b'nodes': node}}
                channel.sendto(bencode(reply), self.client_address)

        self.dht_root = socketserver.UDPServer(('127.0.0.1', 0), DHTRoot)
        self.start(self.dht_root)

    def start(self, server):
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.servers.append((server, thread))

    def close(self):
        for server, thread in reversed(self.servers):
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class ProtocolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = Fixture()

    @classmethod
    def tearDownClass(cls):
        cls.fixture.close()

    def setUp(self):
        self.fixture.corrupt_piece = False
        self.fixture.corrupt_metadata = False
        self.fixture.wrong_bitfield = False

    def test_magnet_hex_and_base32(self):
        digest = self.fixture.meta.info_hash
        for text in (digest.hex(), base64.b32encode(digest).decode('ascii')):
            parsed = parse_magnet('magnet:?xt=urn:btih:' + text + '&dn=Film')
            self.assertEqual(parsed['info_hash'], digest)
            self.assertEqual(parsed['name'], 'Film')

    def test_conflicting_magnet_hashes_rejected(self):
        with self.assertRaises(TorrentError):
            parse_magnet('magnet:?xt=urn:btih:' + '00' * 20 + '&xt=urn:btih:' + '11' * 20)

    def test_invalid_base32_rejected(self):
        with self.assertRaises(TorrentError):
            parse_magnet('magnet:?xt=urn:btih:' + '!' * 32)

    def test_compact_public_filter_and_ipv6(self):
        self.assertEqual(compact_peers(self.fixture.compact), [])
        self.assertEqual(compact_peers(self.fixture.compact, allow_local=True), [self.fixture.tcp.server_address])
        ipv6 = socket.inet_pton(socket.AF_INET6, '2606:4700:4700::1111') + struct.pack('!H', 6881)
        self.assertEqual(compact_peers(ipv6, ipv6=True)[0][1], 6881)

    def test_truncated_peer_list_rejected(self):
        with self.assertRaises(TorrentError):
            compact_peers(b'12345')

    def test_http_tracker_real_announce(self):
        peers = http_tracker(self.fixture.http_url, self.fixture.meta.info_hash, peer_id(), allow_local=True)
        self.assertEqual(peers, [self.fixture.tcp.server_address])

    def test_udp_tracker_ignores_wrong_transaction(self):
        peers = udp_tracker(self.fixture.udp_url, self.fixture.meta.info_hash, peer_id(), allow_local=True)
        self.assertEqual(peers, [self.fixture.tcp.server_address])

    def test_multitracker_deduplication(self):
        peers, errors = tracker_peers([self.fixture.http_url, self.fixture.udp_url],
                                      self.fixture.meta.info_hash, timeout=3, allow_local=True)
        self.assertEqual(peers, [self.fixture.tcp.server_address])
        self.assertEqual(errors, [])

    def test_iterative_dht_lookup_real_udp(self):
        peers = dht_peers(self.fixture.meta.info_hash, timeout=2,
                          bootstrap=[self.fixture.dht_root.server_address], allow_local=True)
        self.assertEqual(peers, [self.fixture.tcp.server_address])

    def test_dht_cancellation(self):
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(TorrentError):
            dht_peers(self.fixture.meta.info_hash, bootstrap=[self.fixture.dht_root.server_address], cancel=cancel)

    def test_multiblock_metadata_exchange_real_tcp(self):
        meta = fetch_metadata(self.fixture.meta.info_hash, self.fixture.tcp.server_address)
        self.assertGreater(len(meta.info_bytes), 16384)
        self.assertEqual(meta.info_hash, self.fixture.meta.info_hash)
        self.assertEqual(meta.info_bytes, self.fixture.meta.info_bytes)

    def test_corrupt_metadata_rejected(self):
        self.fixture.corrupt_metadata = True
        with self.assertRaisesRegex(TorrentError, 'info-hash'):
            fetch_metadata(self.fixture.meta.info_hash, self.fixture.tcp.server_address)

    def test_magnet_end_to_end_via_tracker(self):
        uri = 'magnet:?xt=urn:btih:' + self.fixture.meta.info_hash.hex() + '&tr=' + quote(self.fixture.http_url, safe='')
        meta, peers = resolve_magnet(uri, timeout=5, allow_local=True, use_dht=False)
        self.assertEqual(meta.info_hash, self.fixture.meta.info_hash)
        self.assertIn(self.fixture.tcp.server_address, peers)

    def test_persistent_connection_two_pieces(self):
        before = self.fixture.connections
        peer = PeerSession(self.fixture.meta, self.fixture.tcp.server_address)
        try:
            for index in (0, 1):
                self.assertEqual(peer.fetch(index), self.fixture.payload[index * 32768:(index + 1) * 32768])
            self.assertEqual(self.fixture.connections, before + 1)
        finally:
            peer.close()

    def test_corrupt_piece_closes_peer(self):
        self.fixture.corrupt_piece = True
        peer = PeerSession(self.fixture.meta, self.fixture.tcp.server_address)
        try:
            with self.assertRaisesRegex(TorrentError, 'SHA-1'):
                peer.fetch(0)
            self.assertTrue(peer.closed)
        finally:
            peer.close()

    def test_invalid_bitfield_rejected(self):
        self.fixture.wrong_bitfield = True
        with self.assertRaisesRegex(TorrentError, 'bitfield'):
            PeerSession(self.fixture.meta, self.fixture.tcp.server_address)

    def test_pool_streams_seek_ranges_without_webseed(self):
        pool = PeerPool(self.fixture.meta, [self.fixture.tcp.server_address])
        stream = None
        try:
            pool.prepare(timeout=3)
            store = PieceStore(self.fixture.meta, peer_fetcher=pool.fetch, allow_webseeds=False)
            stream = StreamServer(store, 0)
            for first, last in ((0, 65535), (600000, 620000), (1000, 5000)):
                with urlopen(Request(stream.url, headers={'Range': 'bytes={}-{}'.format(first, last)}), timeout=5) as response:
                    self.assertEqual(response.status, 206)
                    self.assertEqual(response.read(), self.fixture.payload[first:last + 1])
            self.assertEqual(store.last_source, 'TCP peer')
        finally:
            if stream:
                stream.close()
            pool.close()

    def test_verified_disk_resume_and_corruption(self):
        calls = []

        def fetch(index):
            calls.append(index)
            start = index * self.fixture.piece_length
            return self.fixture.payload[start:start + self.fixture.meta.piece_size(index)]

        with tempfile.TemporaryDirectory() as folder:
            first = PieceStore(self.fixture.meta, peer_fetcher=fetch, cache_dir=folder)
            first.piece(1)
            second = PieceStore(self.fixture.meta, peer_fetcher=fetch, cache_dir=folder)
            second.piece(1)
            self.assertEqual(calls, [1])
            Path(folder, '1.piece').write_bytes(b'broken')
            third = PieceStore(self.fixture.meta, peer_fetcher=fetch, cache_dir=folder)
            third.piece(1)
            self.assertEqual(calls, [1, 1])

    def test_inflight_piece_requests_share_one_download(self):
        calls = []

        def fetch(index):
            calls.append(index)
            time.sleep(0.03)
            return self.fixture.payload[:self.fixture.piece_length]

        store = PieceStore(self.fixture.meta, peer_fetcher=fetch)
        results = []
        threads = [threading.Thread(target=lambda: results.append(store.piece(0))) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)
        self.assertEqual(len(results), 4)
        self.assertEqual(calls, [0])


if __name__ == '__main__':
    unittest.main(verbosity=2)
