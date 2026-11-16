"""Player-side stalls, peer discovery, start-buffer policy and premature-end recovery."""

import hashlib
import http.client
import importlib.util
import json
from pathlib import Path
import socket
import socketserver
import struct
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'addon/resources/lib'))
import elementum_py.discovery as discovery
from elementum_py.backend import Backend, Redirect
from elementum_py.catalog import Catalog
from elementum_py.container import duration_seconds
from elementum_py.core import Metainfo, PieceStore, TorrentError, bencode
from elementum_py.discovery import (PUBLIC_TRACKERS, dht_peers, load_dht_nodes, peer_id, remember_dht_node,
                                    resolve_magnet, save_dht_nodes, tracker_peers, udp_tracker,
                                    with_public_trackers)
from elementum_py.playback_policy import (classify_failure, prebuffer_done, prebuffer_target, premature_end,
                                          user_message)
from elementum_py.streaming import StreamServer, stream_mime
from test_backend import FakeEngine, FakeKodi, catalog_transport
from test_protocols import Fixture

KIB, MIB = 1024, 1024 * 1024


class StreamStallTests(unittest.TestCase):
    def setUp(self):
        self.length = 16384
        self.payload = b''.join(hashlib.sha1(bytes([index])).digest() * 820 for index in range(6))[:6 * self.length]
        info = {b'name': b'film.mkv', b'length': len(self.payload), b'piece length': self.length,
                b'pieces': b''.join(hashlib.sha1(self.payload[start:start + self.length]).digest()
                                    for start in range(0, len(self.payload), self.length))}
        self.meta = Metainfo(bencode({b'info': info}))
        self.gate = threading.Event()
        self.failing = None
        self.store = PieceStore(self.meta, max_cache_bytes=self.length * 6, peer_fetcher=self.fetch, allow_webseeds=False)
        self.stream = StreamServer(self.store, 0, heartbeat=.05, reserve=4096, drip=64)
        self.addCleanup(self.store.close)
        self.addCleanup(self.stream.close)
        self.addCleanup(self.gate.set)

    def fetch(self, index):
        if index == 2:
            self.gate.wait(5)
            if self.failing:
                raise TorrentError('Synthetic source failure')
        return self.payload[index * self.length:(index + 1) * self.length]

    def open(self):
        connection = http.client.HTTPConnection('127.0.0.1', self.stream.httpd.server_port, timeout=5)
        self.addCleanup(connection.close)
        connection.request('GET', urlsplit(self.stream.url).path, headers={'Range': 'bytes=0-'})
        response = connection.getresponse()
        self.assertEqual(response.status, 206)
        self.assertEqual(response.headers['Content-Type'], 'video/x-matroska')
        return response

    def test_stall_drips_the_verified_reserve_and_the_body_stays_exact(self):
        response = self.open()
        received = bytearray()
        available = 2 * self.length
        while len(received) < available - 4096:
            received += response.read1(65536)
        self.assertEqual(len(received), available - 4096)
        deadline = time.monotonic() + .5
        while time.monotonic() < deadline:
            received += response.read1(65536)
        # Kodi keeps receiving bytes during the stall, so it buffers instead of
        # deciding that the file has ended; nothing unverified is ever sent.
        self.assertGreaterEqual(len(received), available - 4096 + 3 * 64)
        self.assertLess(len(received), available)
        self.assertEqual(bytes(received), self.payload[:len(received)])
        self.gate.set()
        received += response.read()
        self.assertEqual(bytes(received), self.payload)
        stats = self.stream.stats()
        self.assertGreater(stats['dripped_bytes'], 0)
        self.assertGreater(stats['stall_seconds'], 0)
        self.assertEqual(stats['source_errors'], 0)

    def test_source_failure_after_partial_delivery_keeps_every_verified_byte(self):
        self.failing = True
        response = self.open()
        self.gate.set()
        try:
            received = response.read()
        except http.client.IncompleteRead as error:
            received = error.partial
        self.assertEqual(received, self.payload[:2 * self.length])
        self.assertEqual(self.stream.stats()['source_errors'], 1)

    def test_listener_taken_away_by_the_system_is_reopened_on_the_same_port(self):
        self.assertFalse(self.stream.ensure_listening())
        # The orphaned accept loop dies with its socket; that is the simulated fault.
        hook, threading.excepthook = threading.excepthook, lambda arguments: None
        self.addCleanup(setattr, threading, 'excepthook', hook)
        self.stream.httpd.socket.close()      # What a suspended iOS app finds on resume.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not self.stream.ensure_listening():
            time.sleep(.1)
        self.assertEqual(self.stream.stats()['listener_rebuilt'], 1)
        self.gate.set()
        connection = http.client.HTTPConnection('127.0.0.1', self.stream.port, timeout=5)
        self.addCleanup(connection.close)
        connection.request('GET', urlsplit(self.stream.url).path, headers={'Range': 'bytes=100-299'})
        response = connection.getresponse()
        self.assertEqual(response.status, 206)
        self.assertEqual(response.read(), self.payload[100:300])
        self.assertFalse(self.stream.ensure_listening())

    def test_mime_hint_is_given_only_for_this_engines_own_urls(self):
        self.assertEqual(stream_mime(self.stream.url), 'video/x-matroska')
        self.assertEqual(stream_mime('http://127.0.0.1:8080/' + 'a' * 48 + '/video.mp4'), 'video/mp4')
        for url in ('http://example.org/' + 'a' * 48 + '/video.mp4', 'http://127.0.0.1:8080/files/video.mp4',
                    'http://127.0.0.1:8080/' + 'a' * 48 + '/video.exe', '', None):
            self.assertIsNone(stream_mime(url))


class DiscoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = Fixture()

    @classmethod
    def tearDownClass(cls):
        cls.fixture.close()

    def silent_tracker(self):
        hole = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        hole.bind(('127.0.0.1', 0))
        self.addCleanup(hole.close)
        return 'udp://127.0.0.1:{}/announce'.format(hole.getsockname()[1])

    def test_one_dead_tracker_no_longer_delays_the_others(self):
        found = []
        started = time.monotonic()
        peers, _ = tracker_peers([self.silent_tracker(), self.fixture.http_url], self.fixture.meta.info_hash,
                                 timeout=6, allow_local=True, enough=1, found=found.append)
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(peers, [self.fixture.tcp.server_address])
        self.assertEqual(found, [[self.fixture.tcp.server_address]])

    def test_magnet_metadata_starts_before_the_slowest_tracker_answers(self):
        address = self.fixture.tcp.server_address

        def slow(trackers, info_hash, left=0, timeout=10, allow_local=False, cancel=None, enough=None, found=None):
            found([address])
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline and not cancel.is_set():
                time.sleep(.05)
            return [], []

        with patch.object(discovery, 'tracker_peers', slow):
            started = time.monotonic()
            meta, peers = resolve_magnet('magnet:?xt=urn:btih:' + self.fixture.meta.info_hash.hex(),
                                         timeout=8, use_dht=False, allow_local=True)
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(meta.info_hash, self.fixture.meta.info_hash)
        self.assertEqual(peers[0], address)

    def test_saved_metadata_skips_the_metadata_exchange_and_only_finds_peers(self):
        from urllib.parse import quote
        uri = ('magnet:?xt=urn:btih:' + self.fixture.meta.info_hash.hex() +
               '&tr=' + quote(self.fixture.http_url, safe=''))
        report = {}
        with patch.object(discovery, 'PUBLIC_TRACKERS', ()), \
                patch.object(discovery, 'fetch_metadata', side_effect=AssertionError('metadata was fetched again')):
            started = time.monotonic()
            meta, peers = resolve_magnet(uri, timeout=10, allow_local=True, use_dht=False,
                                         known=self.fixture.meta, diagnostics=report)
        self.assertIs(meta, self.fixture.meta)
        self.assertEqual(peers, [self.fixture.tcp.server_address])
        self.assertTrue(report['saved_metadata'])
        self.assertLess(time.monotonic() - started, 3)
        other = Fixture(payload=b'another torrent' * 9000)
        self.addCleanup(other.close)
        with patch.object(discovery, 'PUBLIC_TRACKERS', ()):
            meta, _ = resolve_magnet(uri, timeout=10, allow_local=True, use_dht=False, known=other.meta)
        # Metadata of a different torrent is ignored and fetched properly.
        self.assertEqual(meta.info_hash, self.fixture.meta.info_hash)
        self.assertIsNot(meta, other.meta)

    def test_public_trackers_follow_the_sources_own_trackers(self):
        own = ['udp://own.example:1/announce', PUBLIC_TRACKERS[0]]
        merged = with_public_trackers(own)
        self.assertEqual(merged[:2], own)
        self.assertEqual(len(merged), len(set(merged)))
        self.assertTrue(set(PUBLIC_TRACKERS) <= set(merged))
        many = ['udp://own{}.example:1/announce'.format(number) for number in range(40)]
        self.assertEqual(with_public_trackers(many), many[:32])
        self.assertTrue(any(url.startswith('http://') for url in PUBLIC_TRACKERS))

    def test_remembered_dht_nodes_survive_a_restart_and_reject_bad_rows(self):
        with patch.object(discovery, '_dht_nodes', type(discovery._dht_nodes)()), \
                patch.object(discovery, '_dht_saved', None), tempfile.TemporaryDirectory() as folder:
            path = str(Path(folder) / 'nodes.json')
            remember_dht_node(('8.8.8.8', 6881), bytes(range(20)))
            remember_dht_node(('1.1.1.1', 6881), b'short')
            for number in range(80):
                remember_dht_node(('9.9.{}.9'.format(number), 6881), bytes([number]) * 20)
            self.assertEqual(len(discovery._dht_nodes), 64)
            save_dht_nodes(path)
            rows = json.loads(Path(path).read_text())
            rows[:0] = [['127.0.0.1', 6881, '00' * 20], ['10.0.0.1', 6881, '00' * 20], ['8.8.4.4', 6881, 'zz'], 'junk']
            Path(path).write_text(json.dumps(rows))
            discovery._dht_nodes.clear()
            load_dht_nodes(path)
            self.assertNotIn(('127.0.0.1', 6881), discovery._dht_nodes)
            self.assertNotIn(('10.0.0.1', 6881), discovery._dht_nodes)
            self.assertNotIn(('8.8.4.4', 6881), discovery._dht_nodes)
            Path(path).write_text('not json')
            load_dht_nodes(path)
            load_dht_nodes(path + '.missing')

    def test_dht_lookup_starts_from_remembered_nodes_and_falls_back_to_routers(self):
        root = self.fixture.dht_root.server_address
        with patch.object(discovery, '_dht_nodes', type(discovery._dht_nodes)()):
            remember_dht_node(root, bytes([1]) * 20)
            peers = dht_peers(self.fixture.meta.info_hash, timeout=3, bootstrap=(), allow_local=True, memory=True)
            self.assertEqual(peers, [self.fixture.tcp.server_address])
            self.assertIn(self.fixture.dht_leaf.server_address, discovery._dht_nodes)
        hole = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        hole.bind(('127.0.0.1', 0))
        self.addCleanup(hole.close)
        with patch.object(discovery, '_dht_nodes', type(discovery._dht_nodes)()):
            for number in range(9):
                remember_dht_node(hole.getsockname(), bytes([number]) * 20)
            remember_dht_node(hole.getsockname(), bytes(20))
            peers = dht_peers(self.fixture.meta.info_hash, timeout=5, bootstrap=[root], allow_local=True, memory=True)
            self.assertEqual(peers, [self.fixture.tcp.server_address])

    def test_udp_tracker_is_reached_over_ipv6_when_ipv4_has_no_route(self):
        compact = self.fixture.compact

        class Tracker(socketserver.BaseRequestHandler):
            def handle(self):
                packet, channel = self.request
                action, transaction = struct.unpack_from('!II', packet, 8)
                reply = (struct.pack('!IIQ', 0, transaction, 99) if action == 0 else
                         struct.pack('!IIIII', 1, transaction, 1800, 1, 1) + compact)
                channel.sendto(reply, self.client_address)

        class Server(socketserver.UDPServer):
            address_family = socket.AF_INET6

        try:
            server = Server(('::1', 0), Tracker)
        except OSError:
            self.skipTest('IPv6 loopback is unavailable on this machine')
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            mapped = [(socket.AF_INET, socket.SOCK_DGRAM, 17, '', ('192.0.2.1', port)),
                      (socket.AF_INET6, socket.SOCK_DGRAM, 17, '', ('::1', port, 0, 0))]
            real_socket = socket.socket

            def no_ipv4_route(family, kind):
                channel = real_socket(family, kind)
                if family == socket.AF_INET:
                    channel.close()
                    raise OSError('Network is unreachable')
                return channel

            # An IPv6-only mobile network: the IPv4 address has no route, and the
            # tracker answers with IPv4 peers because it saw the NAT64 gateway.
            with patch.object(discovery.socket, 'getaddrinfo', return_value=mapped), \
                    patch.object(discovery.socket, 'socket', side_effect=no_ipv4_route):
                peers = udp_tracker('udp://tracker.example:{}/announce'.format(port), self.fixture.meta.info_hash,
                                    peer_id(), timeout=3, allow_local=True)
            self.assertEqual(peers, [self.fixture.tcp.server_address])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class PolicyTests(unittest.TestCase):
    def test_start_buffer_follows_the_bitrate_inside_fixed_bounds(self):
        self.assertEqual(prebuffer_target(2 * 1024 * MIB, 32 * MIB, MIB), 8 * MIB)
        self.assertEqual(prebuffer_target(2 * 1024 * MIB, 16 * MIB, MIB), 4 * MIB)
        self.assertEqual(prebuffer_target(2 * 1024 * MIB, 32 * MIB, MIB, wanted=0), 0)
        self.assertEqual(prebuffer_target(3 * MIB, 32 * MIB, MIB), 3 * MIB)
        # A very large first piece is already more than the start buffer.
        self.assertEqual(prebuffer_target(60 * 1024 * MIB, 32 * MIB, 8 * MIB), 0)
        self.assertEqual(prebuffer_target(60 * 1024 * MIB, 64 * MIB, 16 * MIB, wanted=16 * MIB), 0)
        self.assertEqual(prebuffer_target(700 * MIB, 32 * MIB, MIB, bitrate=300 * KIB), 2400 * KIB)
        self.assertEqual(prebuffer_target(700 * MIB, 32 * MIB, 256 * KIB, bitrate=50 * KIB), MIB)
        self.assertEqual(prebuffer_target(60 * 1024 * MIB, 32 * MIB, MIB, bitrate=8 * MIB), 8 * MIB)

    def test_fast_swarm_starts_early_and_a_slow_one_fills_the_buffer(self):
        self.assertTrue(prebuffer_done(8 * MIB, 8 * MIB))
        self.assertTrue(prebuffer_done(int(2.5 * MIB), 8 * MIB, rate=3 * MIB, bitrate=MIB, waited=1.2))
        self.assertFalse(prebuffer_done(int(2.5 * MIB), 8 * MIB, rate=1.2 * MIB, bitrate=MIB, waited=5))
        self.assertFalse(prebuffer_done(int(2.5 * MIB), 8 * MIB, rate=3 * MIB, bitrate=MIB, waited=.5))
        self.assertFalse(prebuffer_done(MIB, 8 * MIB, rate=3 * MIB, bitrate=MIB, waited=5))
        self.assertFalse(prebuffer_done(6 * MIB, 8 * MIB, rate=30 * MIB, bitrate=0, waited=5))

    def test_failures_get_a_stable_reason_and_plain_advice(self):
        cases = {'No torrent peers found from trackers or DHT; try another source': 'no_peers',
                 'Found 12 peers but none supplied usable torrent metadata; try another source': 'metadata',
                 'No usable TCP peers connected': 'peers_unreachable',
                 'No usable piece source: Connected peers are busy and have not granted an upload slot': 'peers_busy',
                 'No usable piece source: No connected peer has this part of the video yet': 'missing_pieces',
                 'This source did not buffer in time; try another source': 'timeout',
                 'No usable piece source: Peers timed out while sending this part of the video': 'timeout',
                 'No usable piece source: Piece SHA-1 verification failed': 'integrity',
                 'No video files found in torrent': 'no_video', 'Cancelled': 'cancelled'}
        for message, code in cases.items():
            self.assertEqual(classify_failure(message), code, message)
        self.assertEqual(classify_failure('boom', 'metadata'), 'metadata')
        self.assertEqual(classify_failure('boom'), 'unknown')
        self.assertIn('busy', user_message('Connected peers are busy and have not granted an upload slot'))
        self.assertEqual(user_message('Use an HTTP(S) torrent URL or a magnet link'),
                         'Use an HTTP(S) torrent URL or a magnet link')
        self.assertEqual(user_message('Cancelled'), 'Cancelled')

    def test_only_an_end_far_from_the_credits_is_premature(self):
        self.assertTrue(premature_end(90, 600))
        self.assertFalse(premature_end(590, 600))
        self.assertFalse(premature_end(5, 600))
        self.assertFalse(premature_end(90, 0))
        self.assertFalse(premature_end(5850, 6000))
        self.assertTrue(premature_end(5700, 6000))


class ContainerTests(unittest.TestCase):
    INFO = b'\x2a\xd7\xb1\x83\x0f\x42\x40' + b'\x44\x89\x88' + struct.pack('>d', 5400000.0)
    MKV = (b'\x1a\x45\xdf\xa3\x84\x42\x86\x81\x01' + b'\x18\x53\x80\x67\x01' + b'\xff' * 7 + b'\xec\x82\x00\x00' +
           b'\x15\x49\xa9\x66' + bytes([0x80 | len(INFO)]) + INFO + bytes(300))

    def test_matroska_duration_with_scale_and_a_memoryview(self):
        self.assertEqual(duration_seconds(self.MKV, b'', '.mkv'), 5400.0)
        self.assertEqual(duration_seconds(memoryview(bytearray(self.MKV)), memoryview(b''), '.webm'), 5400.0)
        short = b'\x44\x89\x84' + struct.pack('>f', 90000.0)
        self.assertEqual(duration_seconds(b'\x15\x49\xa9\x66' + bytes([0x80 | len(short)]) + short, b'', '.mkv'), 90.0)

    def test_mp4_duration_from_either_end_and_both_header_versions(self):
        old = struct.pack('>I4sB3xIIII', 108, b'mvhd', 0, 0, 0, 1000, 6000000) + bytes(80)
        new = struct.pack('>I4sB3xQQIQ', 120, b'mvhd', 1, 0, 0, 90000, 90000 * 3600) + bytes(80)
        self.assertEqual(duration_seconds(bytes(64), bytes(40) + old, '.mp4'), 6000.0)
        self.assertEqual(duration_seconds(bytes(8) + new, b'', '.mov'), 3600.0)

    def test_seek_index_stored_after_the_media_is_located(self):
        from elementum_py.container import index_offset, starts_index
        slow = struct.pack('>I4s', 32, b'ftyp') + bytes(24) + struct.pack('>I4s', 1000, b'mdat') + bytes(200)
        self.assertEqual(index_offset(slow, 5000, '.mp4'), 1032)
        fast = struct.pack('>I4s', 32, b'ftyp') + bytes(24) + struct.pack('>I4s', 500, b'moov') + bytes(200)
        self.assertIsNone(index_offset(fast, 5000, '.mp4'))
        large = struct.pack('>I4s', 32, b'ftyp') + bytes(24) + struct.pack('>I4sQ', 1, b'mdat', 3 * 2 ** 32) + bytes(64)
        self.assertEqual(index_offset(large, 4 * 2 ** 32, '.mov'), 32 + 3 * 2 ** 32)
        seek = b'\x53\xab\x84\x1c\x53\xbb\x6b' + b'\x53\xac\x83' + (123456).to_bytes(3, 'big')
        entry = b'\x4d\xbb' + bytes([0x80 | len(seek)]) + seek
        start = b'\x1a\x45\xdf\xa3\x84\x42\x86\x81\x01' + b'\x18\x53\x80\x67\x01' + b'\xff' * 7
        matroska = start + b'\x11\x4d\x9b\x74' + bytes([0x80 | len(entry)]) + entry + bytes(64)
        self.assertEqual(index_offset(matroska, 10 ** 6, '.mkv'), len(start) + 123456)
        self.assertIsNone(index_offset(matroska, 1000, '.mkv'))           # Beyond the end of the file.
        for head, extension in ((bytes(64), '.mp4'), (b'\x18\x53\x80\x67\x11\x4d\x9b\x74' * 20, '.mkv'), (b'', '.avi')):
            self.assertIsNone(index_offset(head, 10 ** 6, extension))
        self.assertTrue(starts_index(struct.pack('>I4s', 900, b'moov') + bytes(8), '.mp4'))
        self.assertFalse(starts_index(struct.pack('>I4s', 900, b'mdat') + bytes(8), '.mp4'))
        self.assertTrue(starts_index(memoryview(b'\x1c\x53\xbb\x6b\x90' + bytes(8)), '.mkv'))
        self.assertFalse(starts_index(b'', '.mkv'))

    def test_unknown_or_implausible_values_are_ignored(self):
        for head, tail, extension in ((b'\x15\x49\xa9\x66\xff' * 40, b'', '.mkv'), (b'mvhd' * 20, b'mvhd', '.mp4'),
                                      (self.MKV, b'', '.avi'), (b'', b'', '.mkv'), (self.MKV[:40], b'', '.mkv')):
            self.assertIsNone(duration_seconds(head, tail, extension))
        clip = struct.pack('>I4sB3xIIII', 108, b'mvhd', 0, 0, 0, 1000, 5000) + bytes(80)
        self.assertIsNone(duration_seconds(clip, b'', '.mp4'))     # Five seconds is not a film.


class ResumeRequestTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.kodi, self.engine = FakeKodi(), FakeEngine()
        self.backend = Backend(self.folder.name, self.kodi, engine=self.engine)
        self.uri = 'magnet:?xt=urn:btih:' + 'a' * 40
        self.query = {'tmdb': '81747', 'type': 'movie'}
        self.session = {'uri': self.uri, 'title': 'Film', 'playback_query': self.query,
                        'recovery_chain': 'token', 'file_index': 2}
        self.engine.active = self.session
        self.backend.recovery_chains['token'] = {'attempted': {'a' * 40}, 'count': 0, 'when': time.monotonic()}

    def tearDown(self):
        self.backend.downloads.close()
        self.folder.cleanup()

    def test_same_source_is_reopened_at_the_same_position_at_most_twice(self):
        request = self.backend.resume_request(self.session, 352.75)
        query = {key: values[0] for key, values in parse_qs(urlsplit(request).query).items()}
        self.assertEqual(query['uri'], self.uri)
        self.assertEqual((query['_reuse'], query['resume'], query['fallback'], query['oindex']), ('1', '0', '0', '2'))
        self.assertEqual(query['tmdb'], '81747')
        self.assertEqual(self.backend.recovery_chains['token']['position'], 352.75)
        self.assertIsNotNone(self.backend.resume_request(self.session, 400))
        self.assertIsNone(self.backend.resume_request(self.session, 400))
        # Source switching keeps its own separate allowance.
        self.assertEqual(self.backend.recovery_chains['token']['count'], 0)

    def test_resume_is_refused_when_it_cannot_be_right(self):
        self.assertIsNone(self.backend.resume_request(self.session, 5))
        self.assertIsNone(self.backend.resume_request(dict(self.session), 90))       # Not the live session.
        self.session['offline'] = True
        self.assertIsNone(self.backend.resume_request(self.session, 90))
        self.session['offline'] = False
        self.kodi.setting = lambda key: 'false' if key == 'playback_recovery' else ''
        self.assertIsNone(self.backend.resume_request(self.session, 90))
        self.kodi.setting = lambda key: ''
        self.session['recovery_chain'] = 'unknown'
        self.assertIsNone(self.backend.resume_request(self.session, 90))

    def test_live_session_is_reused_without_preparing_the_torrent_again(self):
        request = self.backend.resume_request(self.session, 352.75)
        query = {key: values[0] for key, values in parse_qs(urlsplit(request).query).items()}
        self.backend.catalog = Catalog(self.folder.name + '/catalog', transport=catalog_transport)
        url = 'http://127.0.0.1:1/' + 'b' * 48 + '/video.mkv'
        self.session['stream'] = types.SimpleNamespace(url=url)
        self.engine.reuse = lambda uri, selected: {'url': url, 'title': 'Film', 'file_index': 2}
        self.engine.prepare = lambda *args, **kwargs: self.fail('the torrent was prepared again')
        result = self.backend.play(query)
        self.assertIsInstance(result, Redirect)
        self.assertEqual(result.url, url)
        self.assertEqual(self.backend.pending_resume, 352.75)
        self.assertEqual(self.session['recovery_chain'], 'token')

    def test_plain_play_never_reuses_a_session_by_accident(self):
        self.session['stream'] = types.SimpleNamespace(url='http://127.0.0.1:1/other')
        self.engine.reuse = lambda uri, selected: self.fail('reuse without a recovery chain')
        self.backend.catalog = Catalog(self.folder.name + '/catalog', transport=catalog_transport)
        self.backend.play({'uri': self.uri, '_reuse': '1', 'tmdb': '81747', 'type': 'movie'})
        self.assertEqual(self.engine.prepared['uri'], self.uri)


class ServiceLoopCase(unittest.TestCase):
    """The real service loop with stub Kodi modules, as in the stall recovery tests."""

    def exercise(self, mode, position=90, loops=25, reopens=True, other_video_after=None, server=None, check=True):
        state = {'clock': 0, 'loops': 0, 'opens': [], 'seeks': [], 'errors': [], 'stops': 0,
                 'signalled': False, 'reopened': False, 'resumes': [], 'rates': [], 'backend_closed': 0}
        self.last_state = state    # Also readable when run() ends with an exception.

        def other_video():
            return other_video_after is not None and state['signalled'] and state['loops'] >= other_video_after
        url = 'http://127.0.0.1/stream'
        store = types.SimpleNamespace(network_bytes=1000, update_playback=lambda *args: None)
        session = {'stream': types.SimpleNamespace(url=url), 'store': store, 'uri': 'magnet:?fixture'}
        engine = types.SimpleNamespace(active=session)

        def stop():
            state['stops'] += 1
            engine.active = None

        engine.stop = stop
        backend = types.SimpleNamespace(
            engine=engine, shutdown=threading.Event(), pending_resume=0, state={'history': [{}]},
            source_health=types.SimpleNamespace(
                record=lambda *args, **kwargs: state['rates'].append(kwargs['rate']) if 'rate' in kwargs else None),
            announce_next_episode=lambda *args: None, save=lambda: None,
            recovery_request=lambda session, position: None,
            close=lambda *args: state.__setitem__('backend_closed', state['backend_closed'] + 1))

        def resume_request(session, position):
            state['resumes'].append(position)
            return 'plugin://plugin.video.elementum/play?_reuse=1'

        backend.resume_request = resume_request

        class Monitor:
            def waitForAbort(self, seconds):
                state['loops'] += 1
                state['clock'] += 2
                return state['loops'] > loops

        class Player:
            def isPlayingVideo(self):
                if (state['reopened'] and reopens) or state['loops'] < 5:
                    return True
                if not state['signalled']:
                    state['signalled'] = True
                    if mode == 'stopped':
                        self.onPlayBackStopped()
                    elif mode != 'silent':
                        self.onPlayBackEnded()
                return other_video()

            def getPlayingFile(self):
                return 'file:///another-video.mkv' if other_video() else url

            def getTime(self):
                return position

            def getTotalTime(self):
                return 600

            def seekTime(self, value):
                state['seeks'].append(value)

        class Kodi:
            dialog = None

            def __init__(self, *args):
                pass

            def setting(self, key):
                return 'false'

            def clear_ids(self, *args):
                pass

            def publish_ids(self, *args):
                return {}

            def notify(self, *args):
                pass

            def progress_close(self):
                pass

            def error(self, *args):
                state['errors'].append(args)

            def rpc(self, method, params):
                state['opens'].append(params)
                state['reopened'] = True
                backend.pending_resume = position

        stubs = {'xbmc': types.SimpleNamespace(Player=Player, Monitor=Monitor, LOGINFO=1, log=lambda *args: None,
                                               getCondVisibility=lambda key: False),
                 'xbmcaddon': types.SimpleNamespace(Addon=lambda *args: types.SimpleNamespace(getAddonInfo=lambda key: '')),
                 'xbmcgui': types.SimpleNamespace(), 'xbmcvfs': types.SimpleNamespace(translatePath=lambda path: path)}
        name = 'elementum_py._service_end_test'
        path = Path(sys.modules['elementum_py.backend'].__file__).with_name('kodi_service.py')
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, stubs):
            spec.loader.exec_module(module)
            with patch.object(module, 'Kodi', Kodi), patch.object(module, 'Backend', lambda *args: backend), \
                    patch.object(module, 'BackendServer', server or (lambda *args: types.SimpleNamespace(
                        close=lambda: None, release=lambda: None))), \
                    patch.object(module, 'time', types.SimpleNamespace(monotonic=lambda: state['clock'])):
                module.run()
        if check:
            self.assertEqual(state['errors'], [])
        return state


class ServiceEndTests(ServiceLoopCase):
    def test_end_far_from_the_credits_reopens_the_live_session_and_seeks_back(self):
        state = self.exercise('ended')
        self.assertEqual(state['resumes'], [90])
        self.assertEqual(len(state['opens']), 1)
        self.assertEqual(state['seeks'], [90])
        self.assertEqual(state['stops'], 0)

    def test_session_that_never_restarts_is_closed_after_the_grace_period(self):
        state = self.exercise('ended', loops=40, reopens=False)
        self.assertEqual(len(state['opens']), 1)
        self.assertEqual(state['stops'], 0)          # Eighty seconds: still inside the 150-second grace.
        state = self.exercise('ended', loops=100, reopens=False)
        self.assertEqual(len(state['opens']), 1)
        self.assertEqual(state['seeks'], [])
        self.assertEqual(state['stops'], 1)

    def test_real_end_user_stop_and_silent_stop_are_never_reopened(self):
        for mode, position in (('ended', 590), ('stopped', 90), ('silent', 90)):
            with self.subTest(mode=mode, position=position):
                state = self.exercise(mode, position)
                self.assertEqual(state['opens'], [])
                self.assertEqual(state['resumes'], [])
                self.assertEqual(state['stops'], 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
