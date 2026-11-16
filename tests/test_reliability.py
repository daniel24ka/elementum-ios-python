"""Failure injection, concurrent range requests and full engine lifecycle tests."""

import concurrent.futures
import json
from pathlib import Path
import random
import socket
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'addon/resources/lib'))
from elementum_py.core import PieceStore, TorrentError
from elementum_py.engine import TorrentEngine
from elementum_py.peers import MissingPiece, PeerPool
from elementum_py.streaming import StreamServer
from test_protocols import Fixture


class ReliabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = Fixture()

    @classmethod
    def tearDownClass(cls):
        cls.fixture.close()

    def store(self, fetch=None):
        def fixture_piece(index):
            start = index * self.fixture.piece_length
            return self.fixture.payload[start:start + self.fixture.meta.piece_size(index)]

        return PieceStore(self.fixture.meta, max_cache_bytes=65536, peer_fetcher=fetch or fixture_piece,
                          allow_webseeds=False)

    def test_eighty_concurrent_seek_ranges_preserve_bytes_and_cache_bound(self):
        pool = PeerPool(self.fixture.meta, [self.fixture.tcp.server_address])
        store = self.store(pool.fetch)
        stream = None
        try:
            pool.prepare(timeout=3)
            stream = StreamServer(store, 0)
            randomizer = random.Random(20261116)
            ranges = [(start, min(start + 4095, len(self.fixture.payload) - 1))
                      for start in [randomizer.randrange(len(self.fixture.payload)) for _ in range(80)]]

            def request_range(bounds):
                start, end = bounds
                with urlopen(Request(stream.url, headers={'Range': 'bytes={}-{}'.format(start, end)}), timeout=15) as response:
                    self.assertEqual(response.status, 206)
                    self.assertEqual(response.read(), self.fixture.payload[start:end + 1])

            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                list(executor.map(request_range, ranges))
            self.assertLessEqual(len(store.cache), 2)
            self.assertLessEqual(sum(map(len, store.cache.values())), 65536)
        finally:
            if stream:
                stream.close()
            pool.close()
            store.close()

    def test_head_does_not_download_a_piece(self):
        calls = []
        store = self.store(lambda index: calls.append(index))
        stream = StreamServer(store, 0)
        try:
            with urlopen(Request(stream.url, method='HEAD'), timeout=3) as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(int(response.headers['Content-Length']), len(self.fixture.payload))
                self.assertEqual(response.read(), b'')
            self.assertEqual(calls, [])
        finally:
            stream.close()
            store.close()

    def test_bad_ranges_are_416_without_fetching(self):
        calls = []
        store = self.store(lambda index: calls.append(index))
        stream = StreamServer(store, 0)
        try:
            for value in ('items=0-5', 'bytes=999999999-', 'bytes=10-1', 'bytes=-0', 'bytes=0-1,3-4'):
                with self.subTest(value=value), self.assertRaises(HTTPError) as caught:
                    urlopen(Request(stream.url, headers={'Range': value}), timeout=3)
                self.assertEqual(caught.exception.code, 416)
                caught.exception.close()
            self.assertEqual(calls, [])
        finally:
            stream.close()
            store.close()

    def test_unknown_stream_token_and_non_ascii_path_are_404(self):
        store = self.store()
        stream = StreamServer(store, 0)
        try:
            with self.assertRaises(HTTPError) as caught:
                urlopen(stream.url.replace(stream.token, 'wrong'), timeout=3)
            self.assertEqual(caught.exception.code, 404)
            caught.exception.close()
            with socket.create_connection(('127.0.0.1', stream.httpd.server_port), timeout=3) as channel:
                channel.sendall(b'GET /\xff HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
                self.assertIn(b' 404 ', channel.recv(1024).split(b'\r\n')[0])
        finally:
            stream.close()
            store.close()

    def test_unavailable_source_returns_503_before_success_headers(self):
        def unavailable(index):
            raise TorrentError('Synthetic disconnected source')

        store = self.store(unavailable)
        stream = StreamServer(store, 0)
        try:
            with self.assertRaises(HTTPError) as caught:
                urlopen(Request(stream.url, headers={'Range': 'bytes=0-999'}), timeout=3)
            self.assertEqual(caught.exception.code, 503)
            caught.exception.close()
        finally:
            stream.close()
            store.close()

    def test_cancelled_inflight_waiters_exit_and_do_not_cache_bytes(self):
        entered, release = threading.Event(), threading.Event()
        errors = []

        def fetch(index):
            entered.set()
            release.wait(2)
            return self.fixture.payload[:32768]

        store = self.store(fetch)

        def read():
            try:
                store.piece(0)
            except TorrentError:
                errors.append('cancelled')

        owner = threading.Thread(target=read)
        waiter = threading.Thread(target=read)
        owner.start()
        self.assertTrue(entered.wait(1))
        waiter.start()
        store.cancel.set()
        waiter.join(timeout=1)
        release.set()
        owner.join(timeout=1)
        self.assertEqual(errors, ['cancelled', 'cancelled'])
        self.assertFalse(store.cache)
        self.assertFalse(store.inflight)

    def test_three_real_engine_start_stream_stop_cycles(self):
        with tempfile.TemporaryDirectory() as profile:
            engine = TorrentEngine(profile, lambda key: '1' if key == 'download_storage' else '')
            with patch('elementum_py.engine.resolve_magnet', return_value=(self.fixture.meta, [self.fixture.tcp.server_address])) as metadata:
                for _ in range(3):
                    prepared = engine.prepare('magnet:?xt=urn:btih:' + self.fixture.meta.info_hash.hex(), peer_only=True)
                    with urlopen(Request(prepared['url'], headers={'Range': 'bytes=0-2047'}), timeout=5) as response:
                        self.assertEqual(response.read(), self.fixture.payload[:2048])
                    self.assertTrue(engine.stats()['active'])
                    engine.stop()
                    engine.stop()
                    self.assertEqual(engine.stats(), {'active': False})
                    with self.assertRaises(OSError):
                        urlopen(prepared['url'], timeout=1)
                self.assertEqual(metadata.call_count, 1)
                self.assertTrue(engine.last_metadata_diagnostics['cache_hit'])

    def test_stop_interrupts_pending_metadata_preparation(self):
        entered = threading.Event()
        errors = []

        def metadata(uri, cancel, **kwargs):
            entered.set()
            cancel.wait(2)
            raise TorrentError('Cancelled')

        with tempfile.TemporaryDirectory() as profile:
            engine = TorrentEngine(profile)

            def prepare():
                try:
                    engine.prepare('magnet:?xt=urn:btih:' + self.fixture.meta.info_hash.hex())
                except TorrentError:
                    errors.append('cancelled')

            with patch('elementum_py.engine.resolve_magnet', side_effect=metadata):
                worker = threading.Thread(target=prepare)
                worker.start()
                self.assertTrue(entered.wait(1))
                engine.stop()
                worker.join(timeout=1)
                self.assertFalse(worker.is_alive())
                self.assertEqual(errors, ['cancelled'])
                self.assertIsNone(engine.active)
                self.assertIsNone(engine.preparing)

    def test_stop_closes_pool_while_socket_preparation_is_pending(self):
        entered, released = threading.Event(), threading.Event()
        errors = []
        class WaitingPool:
            def __init__(self, *args, **kwargs):
                self.closed = False
            def prepare(self, timeout):
                entered.set()
                released.wait(5)
                raise TorrentError('Cancelled')
            def close(self):
                self.closed = True
                released.set()
            def stats(self):
                return {'connected_peers': 0}
        with tempfile.TemporaryDirectory() as profile:
            runtime = TorrentEngine(profile)
            def prepare():
                try:
                    runtime.prepare('magnet:?xt=urn:btih:' + self.fixture.meta.info_hash.hex())
                except TorrentError:
                    errors.append('cancelled')
            with patch('elementum_py.engine.resolve_magnet', return_value=(self.fixture.meta, [self.fixture.tcp.server_address])), \
                    patch('elementum_py.engine.PeerPool', WaitingPool):
                worker = threading.Thread(target=prepare)
                worker.start()
                self.assertTrue(entered.wait(1))
                runtime.stop()
                worker.join(timeout=1)
                self.assertFalse(worker.is_alive())
                self.assertEqual(errors, ['cancelled'])

    def test_one_upload_slot_is_reserved_for_foreground_reads_and_seeks(self):
        with tempfile.TemporaryDirectory() as profile:
            runtime = TorrentEngine(profile)
            with patch('elementum_py.engine.resolve_magnet', return_value=(self.fixture.meta, [self.fixture.tcp.server_address])):
                try:
                    runtime.prepare('magnet:?xt=urn:btih:' + self.fixture.meta.info_hash.hex(), peer_only=True)
                    store = runtime.active['store']
                    store.prefetch([3, 4, 5])
                    self.assertFalse(store.prefetch_futures)
                    self.assertEqual(b''.join(store.read(0, 400000, 20000)), self.fixture.payload[400000:420000])
                finally:
                    runtime.stop()

    def test_unavailable_video_index_fails_before_player_url_is_published(self):
        with tempfile.TemporaryDirectory() as profile:
            runtime = TorrentEngine(profile, lambda key: 'false' if key == 'dht_enabled' else '')
            last = len(self.fixture.meta.hashes) - 1
            self.fixture.corrupt_piece_indices = {last}
            try:
                with patch('elementum_py.engine.resolve_magnet', return_value=(self.fixture.meta, [self.fixture.tcp.server_address])):
                    with self.assertRaises(TorrentError):
                        runtime.prepare('magnet:?xt=urn:btih:' + self.fixture.meta.info_hash.hex(), peer_only=True)
                self.assertIsNone(runtime.active)
                self.assertEqual(runtime.last_preparation['stage'], 'video_index')
            finally:
                self.fixture.corrupt_piece_indices = set()
                runtime.stop()

    def test_pool_replaces_eight_stale_peers_and_stops_idle_workers(self):
        import elementum_py.peers as peers
        payload = self.fixture.payload[:self.fixture.piece_length]
        attempts = []

        class Peer:
            def __init__(self, meta, address, cancel):
                self.address = address
                self.busy = threading.Lock()
                self.closed = False
                self.bytes_received, self.elapsed = 0, 1
                attempts.append(address[1])

            def fetch(self, index, timeout):
                if self.address[1] < 9:
                    raise MissingPiece('Synthetic peer has other pieces')
                self.bytes_received += len(payload)
                return payload

            def close(self):
                self.closed = True

        addresses = [('127.0.0.1', index) for index in range(1, 13)]
        with patch.object(peers, 'PeerSession', Peer), patch.object(peers.random.SystemRandom, 'shuffle', lambda *args: None):
            pool = PeerPool(self.fixture.meta, addresses)
            try:
                pool.prepare(timeout=2)
                self.assertEqual(pool.fetch(0, timeout=3), payload)
                self.assertTrue(any(index >= 9 for index in attempts))
                self.assertLessEqual(pool.stats()['connected_peers'], 8)
            finally:
                pool.close()
            self.assertFalse(any(worker.is_alive() for worker in pool.connectors))

    def test_verified_metadata_peer_is_not_lost_in_random_stale_endpoints(self):
        import elementum_py.peers as peers
        addresses = [('127.0.0.1', index) for index in range(1, 201)]
        good = self.fixture.tcp.server_address
        with patch.object(peers.random.SystemRandom, 'shuffle', lambda instance, items: items.reverse()):
            pool = PeerPool(self.fixture.meta, addresses + [good], priority=[good])
            try:
                self.assertEqual(pool.addresses[0], good)
                pool.prepare(timeout=2)
                self.assertEqual(pool.fetch(0, timeout=3), self.fixture.payload[:self.fixture.piece_length])
            finally:
                pool.close()


if __name__ == '__main__':
    unittest.main()
