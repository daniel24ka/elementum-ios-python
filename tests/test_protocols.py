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
from unittest.mock import patch
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
    def __init__(self, payload=None, piece_length=32768, hashes=None):
        self.payload = payload if payload is not None else bytes(range(251)) * 3307
        self.piece_length = piece_length
        self.info = {b'name': b'fixture.mp4', b'length': len(self.payload),
                     b'piece length': self.piece_length, b'source': b'fixture' * 3000,
                     b'pieces': hashes if hashes is not None else b''.join(
                         hashlib.sha1(self.payload[offset:offset + self.piece_length]).digest()
                         for offset in range(0, len(self.payload), self.piece_length))}
        self.meta = Metainfo(bencode({b'info': self.info}))
        self.connections = 0
        self.corrupt_piece = False
        self.corrupt_metadata = False
        self.wrong_bitfield = False
        self.unsorted_control = False
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
                    header = bencode({b'm': {b'ut_metadata': 7}, b'metadata_size': len(owner.meta.info_bytes)})
                    if owner.unsorted_control:
                        header = b'd13:metadata_size' + bencode(len(owner.meta.info_bytes)) + b'1:md11:ut_metadatai7eee'
                    send_message(self.request, b'\x14\x00' + header)
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
        self.fixture.unsorted_control = False

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

    def test_unsorted_peer_handshake_metadata_stays_verified(self):
        self.fixture.unsorted_control = True
        meta = fetch_metadata(self.fixture.meta.info_hash, self.fixture.tcp.server_address)
        self.assertEqual(meta.info_bytes, self.fixture.meta.info_bytes)
        peer = PeerSession(self.fixture.meta, self.fixture.tcp.server_address)
        try:
            self.assertEqual(peer.fetch(0), self.fixture.payload[:self.fixture.piece_length])
        finally:
            peer.close()

    def test_relaxed_control_decoder_still_rejects_duplicate_keys(self):
        self.assertEqual(bdecode(b'd1:bi2e1:ai1ee', strict=False), {b'a': 1, b'b': 2})
        with self.assertRaises(TorrentError):
            bdecode(b'd1:ai1e1:ai2ee', strict=False)
        with self.assertRaises(TorrentError):
            bdecode(b'd1:bi2e1:ai1ee')

    def test_dht_is_used_even_when_tracker_has_stale_peers(self):
        import elementum_py.discovery as discovery
        real_fetch = discovery.fetch_metadata
        stale = [('198.51.100.' + str(number), 1234) for number in range(1, 5)]

        def fetch(info_hash, address, trackers, timeout):
            if address in stale:
                raise TorrentError('Synthetic stale peer')
            return real_fetch(info_hash, address, trackers, timeout)

        with patch.object(discovery, 'tracker_peers', return_value=(stale, [])), \
                patch.object(discovery, 'dht_peers', return_value=[self.fixture.tcp.server_address]) as dht, \
                patch.object(discovery, 'fetch_metadata', side_effect=fetch):
            meta, _ = resolve_magnet('magnet:?xt=urn:btih:' + self.fixture.meta.info_hash.hex(), timeout=4)
            self.assertEqual(meta.info_hash, self.fixture.meta.info_hash)
            dht.assert_called_once()

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

    def test_prefetch_future_does_not_retain_piece_bytes(self):
        store = PieceStore(self.fixture.meta, peer_fetcher=lambda index: self.fixture.payload[:32768])
        try:
            store.prefetch([0])
            future = store.prefetch_futures[0]
            self.assertIsNone(future.result(timeout=2))
            self.assertEqual(store.piece(0), self.fixture.payload[:32768])
        finally:
            store.close()


class LargePieceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.payload = (bytes(range(251)) * (8 * 1024 * 1024 // 251 + 100))[:8 * 1024 * 1024 + 12345]
        cls.fixture = Fixture(cls.payload, 8 * 1024 * 1024)

    @classmethod
    def tearDownClass(cls):
        cls.fixture.close()

    def test_eight_mib_piece_metadata_and_cross_boundary_stream(self):
        meta = fetch_metadata(self.fixture.meta.info_hash, self.fixture.tcp.server_address)
        self.assertEqual(meta.piece_length, 8 * 1024 * 1024)
        pool = PeerPool(meta, [self.fixture.tcp.server_address])
        store = PieceStore(meta, peer_fetcher=pool.fetch, allow_webseeds=False)
        stream = None
        try:
            pool.prepare(timeout=3)
            stream = StreamServer(store, 0)
            start, end = 8 * 1024 * 1024 - 4096, len(self.payload) - 1
            with urlopen(Request(stream.url, headers={'Range': 'bytes={}-{}'.format(start, end)}), timeout=12) as response:
                self.assertEqual(response.read(), self.payload[start:end + 1])
            self.assertLessEqual(store.prefetch_workers, 2)
        finally:
            if stream:
                stream.close()
            pool.close()
            store.close()

    def test_large_piece_prefetch_concurrency_is_bounded(self):
        info = {b'name': b'fixture.mp4', b'piece length': 32 * 1024 * 1024,
                b'length': 1, b'pieces': hashlib.sha1(b'x').digest()}
        meta = Metainfo(bencode({b'info': info}))
        self.assertEqual(PieceStore(meta).prefetch_workers, 1)
        info[b'piece length'] = 128 * 1024 * 1024
        with self.assertRaises(TorrentError):
            Metainfo(bencode({b'info': info}))


class LargeFileAddressingTests(unittest.TestCase):
    def test_24_gib_torrent_real_tcp_seek_beyond_20_gib(self):
        length = 24 * 1024 ** 3 + 1024
        piece_length = 16 * 1024 ** 2

        class SparseZeros:
            def __len__(self):
                return length

            def __getitem__(self, index):
                # The synthetic peer only generates a requested 16 KiB block.
                start, stop, step = index.indices(length)
                if step != 1 or stop - start > piece_length:
                    raise AssertionError('Unbounded synthetic allocation')
                return bytes(stop - start)

        digest = hashlib.sha1(bytes(piece_length)).digest()
        hashes = digest * (length // piece_length) + hashlib.sha1(bytes(length % piece_length)).digest()
        fixture = Fixture(SparseZeros(), piece_length, hashes)
        pool = PeerPool(fixture.meta, [fixture.tcp.server_address])
        store = PieceStore(fixture.meta, peer_fetcher=pool.fetch, allow_webseeds=False)
        # Prefetch is tested separately. This case measures exact large offsets.
        store.prefetch = lambda indexes: None
        stream = None
        try:
            pool.prepare(timeout=3)
            stream = StreamServer(store, 0)
            start = 21 * 1024 ** 3 + 1024 ** 2
            with urlopen(Request(stream.url, headers={'Range': 'bytes={}-{}'.format(start, start + 8191)}), timeout=20) as response:
                self.assertEqual(response.status, 206)
                self.assertEqual(response.headers['Content-Range'], 'bytes {}-{}/{}'.format(start, start + 8191, length))
                self.assertEqual(response.read(), bytes(8192))
            self.assertEqual(fixture.meta.total_length, length)
            self.assertEqual(pool.stats()['peer_bytes_received'], piece_length)
        finally:
            if stream:
                stream.close()
            pool.close()
            store.close()
            fixture.close()


if __name__ == '__main__':
    unittest.main(verbosity=2)
