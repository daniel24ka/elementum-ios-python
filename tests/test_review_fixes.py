"""One regression test for each defect confirmed by the independent review of 0.3.4~dev1."""

import hashlib
import http.client
import importlib.util
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

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'addon/resources/lib'))
sys.path.insert(0, str(ROOT / 'tools'))
import elementum_py.discovery as discovery
import elementum_py.peers as peers_module
from elementum_py.container import duration_seconds
from elementum_py.core import Abandoned, Metainfo, PieceStore, TorrentError, bencode
from elementum_py.discovery import peer_id, resolve_magnet, tracker_peers, udp_tracker
from elementum_py.engine import TorrentEngine
from elementum_py.peers import PeerPool, PieceJob
from elementum_py.playback_policy import classify_failure, user_message
import test_swarm
import test_upgrades
from test_protocols import Fixture
from test_swarm import KIB, MIB, Scripted, Stub, rates

BLOCK = peers_module.BLOCK


class QuietPeerTests(test_swarm.SchedulerCase):
    """Peers that stop delivering must neither be trusted nor written off for good."""

    def test_rejects_that_follow_a_choke_are_routine_and_the_unchoke_restores_the_peer(self):
        peer = Stub(self.swarm, 32 * 16384, 'fast-extension')
        peer.fast = True
        job = self.job(0, urgent=True, wanted=12)
        self.assign()
        self.assertEqual(len(peer.outstanding), 12)
        self.swarm.choke(peer, True)
        for block in range(12):                       # What BEP 6 requires of a choking peer.
            self.swarm.rejected(peer, 0, block * BLOCK)
        self.assertFalse(peer.snubbed)
        self.assertEqual(peer.outstanding, {})
        peer.sent.clear()
        self.swarm.choke(peer, False)
        self.assertEqual(peer.requested(0), list(range(12)))
        self.assertEqual(sorted(job.holders), list(range(12)))

    def test_rejects_from_an_unchoked_peer_still_count_and_a_new_upload_slot_clears_them(self):
        peer = Stub(self.swarm, 32 * 16384, 'refuser')
        peer.fast = True
        self.job(0, urgent=False, wanted=12)
        self.assign()
        for block in range(8):
            self.swarm.rejected(peer, 0, block * BLOCK)
        self.assertTrue(peer.snubbed)
        self.swarm.rejected(peer, 5, 0)               # Never requested: says nothing about the peer.
        self.swarm.choke(peer, True)
        self.swarm.choke(peer, False)
        self.assertFalse(peer.snubbed)
        self.assertEqual(peer.rejects, 0)

    def test_quiet_fast_peer_does_not_make_a_slower_one_leave_the_piece_to_it(self):
        silent = Stub(self.swarm, 128 * 16384, 'silent', depth=64)
        slow = Stub(self.swarm, 8 * 16384, 'slow', depth=8)
        silent.suspect = True
        self.job(0, urgent=True, wanted=4)
        self.assign()
        self.assertEqual(slow.requested(0), [0, 1, 2, 3])
        self.assertEqual(silent.requested(0), [])

    def test_retry_from_one_peer_is_not_left_to_faster_peers_that_may_not_deliver_it(self):
        fast = Stub(self.swarm, 128 * 16384, 'fast', depth=64)
        chosen = Stub(self.swarm, 8 * 16384, 'chosen', depth=8)
        job = self.job(0, urgent=True, wanted=4)
        job.exclusive = chosen
        self.assign()
        self.assertEqual(chosen.requested(0), [0, 1, 2, 3])
        self.assertEqual(fast.requested(0), [])
        # When the chosen peer stops serving, another one is picked for the retry.
        chosen.choked = True
        self.assertIs(self.swarm._clean_peer(job), fast)
        fast.snubbed = True
        self.assertIsNone(self.swarm._clean_peer(job))

    def test_choked_peer_serving_an_allowed_fast_piece_keeps_its_requests(self):
        allowed = Stub(self.swarm, 16 * 16384, 'allowed')
        other = Stub(self.swarm, 16 * 16384, 'other')
        allowed.fast, allowed.choked, allowed.allowed_fast = True, True, {0}
        other.complete = False                        # Has nothing: only there to make full passes run.
        job = self.job(0, urgent=True, wanted=4)
        for _ in range(4):
            job.hedge_after = 0
            self.assign()
        self.assertEqual(allowed.requested(0), [0, 1, 2, 3])      # Once each, not once per pass.
        self.assertEqual(allowed.cancelled(0), [])
        self.assertEqual(len(allowed.outstanding), 4)
        self.assertEqual(allowed.stale, {})

    def test_quiet_peer_with_nothing_outstanding_gets_three_more_chances_then_is_closed(self):
        peer = Stub(self.swarm, 16 * 16384, 'limbo')
        clock = self.now
        for chance in (1, 2, 3):
            peer.suspect, peer.quiet_since = True, clock
            clock += peers_module.SECOND_CHANCE + 1
            self.swarm.last_tick = 0
            self.swarm.tick(clock)
            self.assertFalse(peer.suspect)
            self.assertEqual(peer.chances, chance)
            self.assertFalse(peer.closed)
        peer.suspect, peer.quiet_since = True, clock
        self.swarm.last_tick = 0
        self.swarm.tick(clock + peers_module.SECOND_CHANCE + 1)
        self.assertTrue(peer.closed)

    def test_delivered_block_resets_the_count_of_used_chances(self):
        peer = Stub(self.swarm, 16 * 16384, 'recovered')
        job = self.job(0, urgent=False, wanted=1)
        peer.chances = 3
        self.hold(peer, job, 0, arrives_in=.1)
        self.swarm.block(peer, 0, 0, bytes(BLOCK))
        self.assertEqual(peer.chances, 0)

    def test_abandon_during_a_failed_hash_check_ends_the_fetch_at_once(self):
        job = PieceJob(self.meta, 0, False)
        job.authors = [Stub(self.swarm, 16 * 16384, 'author')] * job.count
        job.abandoned = True
        job.finished.set()
        with self.assertRaises(Abandoned):
            self.swarm._failed(job)
        self.assertTrue(job.finished.is_set())

    def test_fetch_waits_out_the_moment_between_a_drop_and_its_redial(self):
        started = time.monotonic()
        with self.assertRaises(TorrentError):
            self.swarm.fetch(0, timeout=5)
        # With no peer and nothing pending it still fails early, but not on the first poll.
        self.assertGreater(time.monotonic() - started, .5)
        self.assertLess(time.monotonic() - started, 3)

    def test_banned_address_leaves_the_redial_list_and_its_other_session_is_closed(self):
        pool = PeerPool(self.meta, [])
        self.addCleanup(pool.close)
        liar, again = Stub(pool.swarm, 16 * 16384, 'liar'), Stub(pool.swarm, 16 * 16384, 'liar')
        honest = Stub(pool.swarm, 16 * 16384, 'honest')
        pool.retries = [(0, liar.address), (0, ('198.51.100.7', 6881))]
        pool.swarm._ban(liar)
        self.assertTrue(liar.closed and again.closed)
        self.assertFalse(honest.closed)
        self.assertEqual(pool._next_address(peers_module.clock()), ('198.51.100.7', 6881))
        self.assertIsNone(pool._next_address(peers_module.clock()))
        self.assertEqual(pool.swarm.counters['banned_peers'], 1)


class RealSocketFixTests(test_swarm.SwarmCase):
    def test_fast_extension_peer_serves_again_after_choking_in_mid_piece(self):
        # libtorrent-family clients choke on every upload-slot rotation and reject
        # what was queued. With one such seeder this stopped playback before the fix.
        pool = self.start([dict(rate=512 * KIB, fast=True, choke_at=1.0, choke_for=1.0)], size=2 * MIB, piece=2 * MIB)
        started = time.monotonic()
        self.assertEqual(pool.fetch(0, timeout=20), self.scripted.content(0))
        self.assertLess(time.monotonic() - started, 9)
        stats = pool.stats()
        self.assertEqual(stats['snubbed_peers'], 0)
        self.assertEqual(stats['connected_peers'], 1)

    def test_peer_that_does_not_read_cannot_hold_up_the_scheduler(self):
        pool = self.start(rates(2, MIB), size=2 * MIB, piece=MIB)
        stuck = pool.sessions[0]
        stuck.send_lock.acquire()                    # Its reader thread is blocked in a send.
        self.addCleanup(lambda: stuck.send_lock.locked() and stuck.send_lock.release())
        with patch.object(peers_module, 'SEND_PATIENCE', .2):
            started = time.monotonic()
            pool.swarm._flush({stuck: bytearray(4)})
            self.assertLess(time.monotonic() - started, 1.5)
        self.assertTrue(stuck.closed)
        self.assertEqual(pool.fetch(0, timeout=10), self.scripted.content(0))

    def test_flood_of_requests_from_a_peer_gets_a_bounded_number_of_answers(self):
        pool = self.start(rates(1, MIB), size=MIB, piece=MIB)
        peer = pool.sessions[0]
        peer.fast, peer.extension_ids = True, {b'ut_metadata': 3}
        sent = []
        peer._send = sent.append
        for _ in range(500):
            peer._message(memoryview(struct.pack('!BIII', 6, 0, 0, BLOCK)))
            peer._message(memoryview(b'\x14\x01' + bencode({b'msg_type': 0, b'piece': 0})))
        pieces = (len(self.scripted.meta.info_bytes) + 16383) // 16384
        self.assertEqual(len(sent), 2 * pieces + 64)


class DiscoveryFixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = Fixture()

    @classmethod
    def tearDownClass(cls):
        cls.fixture.close()

    def test_broken_tracker_reply_does_not_cost_the_other_trackers(self):
        real = discovery.http_tracker

        def tracker(url, *arguments):
            if 'broken' in url:
                raise http.client.BadStatusLine('not HTTP')
            if 'odd' in url:
                raise TypeError('peers was a list of strings')
            return real(url, *arguments)

        with patch.object(discovery, 'http_tracker', tracker):
            peers, errors = tracker_peers(['http://broken.example/announce', 'http://odd.example/announce',
                                           self.fixture.http_url], self.fixture.meta.info_hash, timeout=4, allow_local=True)
        self.assertEqual(peers, [self.fixture.tcp.server_address])
        self.assertEqual(sorted(errors), ['BadStatusLine', 'TypeError'])

    def test_magnet_resolution_survives_an_unexpected_error_from_discovery(self):
        def trackers(*arguments, found=None, **options):
            found([self.fixture.tcp.server_address])
            raise KeyError('malformed reply')

        with patch.object(discovery, 'tracker_peers', trackers):
            meta, peers = resolve_magnet('magnet:?xt=urn:btih:' + self.fixture.meta.info_hash.hex(),
                                         timeout=6, use_dht=False, allow_local=True)
        self.assertEqual(meta.info_hash, self.fixture.meta.info_hash)

    def ipv6_tracker(self, payload):
        class Tracker(socketserver.BaseRequestHandler):
            def handle(self):
                packet, channel = self.request
                action, transaction = struct.unpack_from('!II', packet, 8)
                reply = (struct.pack('!IIQ', 0, transaction, 99) if action == 0 else
                         struct.pack('!IIIII', 1, transaction, 1800, 1, 1) + payload)
                channel.sendto(reply, self.client_address)

        class Server(socketserver.UDPServer):
            address_family = socket.AF_INET6

        try:
            server = Server(('::1', 0), Tracker)
        except OSError:
            self.skipTest('IPv6 loopback is unavailable on this machine')
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        port = server.server_address[1]
        mapped = [(socket.AF_INET6, socket.SOCK_DGRAM, 17, '', ('::1', port, 0, 0))]
        with patch.object(discovery.socket, 'getaddrinfo', return_value=mapped):
            return udp_tracker('udp://tracker.example:{}/announce'.format(port), self.fixture.meta.info_hash,
                               peer_id(), timeout=3)

    def test_native_ipv6_peer_list_is_not_also_read_as_ipv4(self):
        hosts = ['2606:4700:4700::1111', '2001:4860:4860::8888', '2620:fe::fe']
        payload = b''.join(socket.inet_pton(socket.AF_INET6, host) + struct.pack('!H', 6881) for host in hosts)
        self.assertEqual(self.ipv6_tracker(payload), [(host, 6881) for host in hosts])

    def test_ipv4_peer_list_behind_nat64_is_still_read_as_ipv4(self):
        hosts = ['93.184.216.34', '151.101.1.69', '104.16.132.229', '8.8.8.8', '1.1.1.1', '9.9.9.9']
        payload = b''.join(socket.inet_aton(host) + struct.pack('!H', 51413) for host in hosts)
        self.assertEqual(len(payload) % 18, 0)       # Six IPv4 entries are also a whole number of IPv6 ones.
        self.assertEqual(self.ipv6_tracker(payload), [(host, 51413) for host in hosts])


class RecordingScheduler:
    """Stands in for the swarm: piece 5 arrives only when the test says so."""

    def __init__(self, payload, length):
        self.payload, self.length = payload, length
        self.gate = threading.Event()
        self.fetches, self.abandoned, self.demoted, self.promoted = [], [], [], []
        self.dropped = threading.Event()

    def fetch(self, index, urgent):
        self.fetches.append(index)
        if index == 5:
            while not self.gate.wait(.02):
                if self.dropped.is_set():
                    self.dropped.clear()
                    raise Abandoned('Abandoned read-ahead piece')
        return self.payload[index * self.length:(index + 1) * self.length]

    def promote(self, index):
        self.promoted.append(index)

    def demote(self, index):
        self.demoted.append(index)

    def abandon(self, index):
        self.abandoned.append(index)
        self.dropped.set()
        return True

    def focus(self, index):
        pass


class ReaderFixTests(unittest.TestCase):
    def setUp(self):
        self.length = 16384
        self.payload = b''.join(hashlib.sha1(bytes([index])).digest() * 820 for index in range(120))[:120 * self.length]
        info = {b'name': b'film.mkv', b'length': len(self.payload), b'piece length': self.length,
                b'pieces': b''.join(hashlib.sha1(self.payload[start:start + self.length]).digest()
                                    for start in range(0, len(self.payload), self.length))}
        self.meta = Metainfo(bencode({b'info': info}))
        self.scheduler = RecordingScheduler(self.payload, self.length)
        self.store = PieceStore(self.meta, max_cache_bytes=self.length * 12, scheduler=self.scheduler,
                                allow_webseeds=False)
        self.addCleanup(self.store.close)
        self.addCleanup(self.scheduler.gate.set)

    def wait(self, condition):
        deadline = time.monotonic() + 3
        while not condition() and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(condition())

    def test_two_plain_readers_of_one_piece_do_not_cancel_each_other(self):
        first = self.store.read(0, 5 * self.length, 100, heartbeat=.02)
        second = self.store.read(0, 5 * self.length + 200, 100, heartbeat=.02)
        self.assertEqual(next(first), b'')
        self.assertEqual(next(second), b'')
        first.close()                                  # One connection goes away during the stall.
        self.assertEqual(self.scheduler.abandoned, [])
        self.scheduler.gate.set()
        self.assertEqual(b''.join(bytes(chunk) for chunk in second),
                         self.payload[5 * self.length + 200:5 * self.length + 300])
        self.assertEqual(self.scheduler.fetches.count(5), 1)

    def test_last_plain_reader_closing_gives_the_piece_up_without_starting_it_again(self):
        only = self.store.read(0, 5 * self.length, 100, heartbeat=.02)
        self.assertEqual(next(only), b'')
        only.close()
        self.assertEqual(self.scheduler.abandoned, [5])
        self.wait(lambda: 5 not in self.store.inflight)
        time.sleep(.15)
        self.assertEqual(self.scheduler.fetches.count(5), 1)

    def test_reader_closed_after_a_seek_does_not_restart_the_piece_it_waited_for(self):
        self.store.configure_stream(0)
        self.store.prefetch([5])                       # Read-ahead owns the download of piece 5.
        self.wait(lambda: 5 in self.scheduler.fetches)
        stalled = self.store.read(0, 5 * self.length, 20 * self.length, heartbeat=.02)
        self.assertEqual(next(stalled), b'')
        seek = self.store.read(0, 60 * self.length, 20 * self.length, heartbeat=.02)
        self.assertEqual(bytes(next(seek)), self.payload[60 * self.length:61 * self.length])
        self.assertIn(5, self.scheduler.demoted)
        stalled.close()                                # Kodi closes the connection it had parked.
        self.wait(lambda: 5 not in self.store.inflight)
        time.sleep(.15)
        self.assertEqual(self.scheduler.fetches.count(5), 1)
        seek.close()

    def test_older_connection_that_is_read_again_gets_the_window_back(self):
        self.store.configure_stream(0)
        self.store.update_playback(100)
        old = self.store.read(0, 10 * self.length, 60 * self.length)
        next(old)
        new = self.store.read(0, 80 * self.length, 30 * self.length)
        next(new)
        self.assertTrue(all(index >= 80 for index in self.store.window))
        with patch.object(PieceStore, 'PARKED_AFTER', .05):
            time.sleep(.1)                             # The newer connection is open but silent.
            next(old)
            # One burst proves nothing: a parked connection drains a piece too.
            self.assertTrue(all(index >= 80 for index in self.store.window))
            time.sleep(.08)
            next(old)                                  # Still being read: Kodi went back to it.
            self.assertTrue(all(index < 40 for index in self.store.window))
            next(new)                                  # The other one needs the same proof to return.
            self.assertTrue(all(index < 40 for index in self.store.window))
        old.close()
        new.close()


class PrepareFixTests(test_swarm.SwarmCase):
    def test_crafted_matroska_header_is_rejected_in_bounded_time(self):
        crafted = b'\x81\x85\x15\x49\xa9\x66\xff' * 80000        # 560 KB: hours of work before the fix.
        started = time.monotonic()
        self.assertIsNone(duration_seconds(crafted, b'', '.mkv'))
        self.assertLess(time.monotonic() - started, 1)
        noise = b'\x15\x49\xa9\x66\x80' * 8                       # A few false matches before the real element.
        info = b'\x2a\xd7\xb1\x83\x0f\x42\x40' + b'\x44\x89\x88' + struct.pack('>d', 5400000.0)
        self.assertEqual(duration_seconds(noise + b'\x15\x49\xa9\x66' + bytes([0x80 | len(info)]) + info, b'', '.mkv'), 5400.0)

    def test_start_buffer_is_skipped_not_failed_when_the_budget_is_already_spent(self):
        # The optional seek-index fetch may have used the time that was left.
        with tempfile.TemporaryDirectory() as profile:
            engine = TorrentEngine(profile)
            store = types.SimpleNamespace(
                meta=types.SimpleNamespace(piece_length=MIB), bitrate=0, prime=lambda target: None, rate=lambda: 0,
                buffer_stats=lambda: {'ahead_verified_bytes': 0, 'window_verified': False})
            report = engine.buffer_start(store, None, 700 * MIB, 32 * MIB, lambda: -3, threading.Event())
        self.assertEqual(report['target_bytes'], 8 * MIB)
        self.assertEqual(report['buffered_bytes'], 0)
        self.assertLess(report['seconds'], 1)

    def test_cache_of_one_piece_does_not_download_either_end_twice(self):
        self.scripted = Scripted(rates(3, 2 * MIB), size=5 * MIB, piece=MIB, bitrate=128 * KIB)
        self.addCleanup(self.scripted.close)

        class OnePiece(TorrentEngine):
            def number(self, key, default, low, high):
                return 1 if key == 'stream_cache_mb' else super().number(key, default, low, high)

        with tempfile.TemporaryDirectory() as profile:
            engine = OnePiece(profile, lambda key: {'download_storage': '1', 'dht_enabled': 'false'}.get(key, ''))
            uri = 'magnet:?xt=urn:btih:' + self.scripted.meta.info_hash.hex()
            with patch('elementum_py.engine.resolve_magnet', return_value=(self.scripted.meta, self.scripted.addresses)):
                try:
                    engine.prepare(uri, peer_only=True)
                    self.assertEqual(engine.active['store'].limit, 1)
                    self.assertEqual(engine.last_preparation['duration_seconds'], 40.0)
                finally:
                    engine.stop()
        served = {}
        for _, _, index, size in self.scripted.swarm.stats()['blocks']:
            served[index] = served.get(index, 0) + size
        # Once each. A second download would be two mebibytes; a few blocks may be
        # requested twice while the peers' rates are still unknown.
        self.assertLess(served[0], 1.25 * MIB)
        self.assertLess(served[4], 1.25 * MIB)

    def test_unrecognised_early_errors_reach_the_viewer_unchanged(self):
        for message in ('Use an HTTP(S) torrent URL or a magnet link', 'A magnet must contain one unambiguous v1 hash',
                        'Only v1 torrents are supported by this experiment', 'Trailing bencode data'):
            self.assertEqual(user_message(message), message)
        self.assertEqual(classify_failure('Trailing bencode data', 'metadata'), 'metadata')   # Diagnostics only.
        self.assertIn('none sent the torrent details',
                      user_message('Found 3 peers but none supplied usable torrent metadata; try another source'))


class ServiceFixTests(test_upgrades.ServiceLoopCase):
    def test_another_video_after_a_failed_reopen_ends_the_waiting_session(self):
        # The reopened stream never starts; the viewer plays something else instead.
        state = self.exercise('ended', loops=100, reopens=False, other_video_after=10)
        self.assertEqual(len(state['opens']), 1)
        self.assertEqual(state['stops'], 1)

    def test_reopened_session_measures_its_rate_from_the_reopen_onwards(self):
        state = self.exercise('ended', loops=40)
        self.assertEqual(state['seeks'], [90])
        self.assertTrue(state['rates'])
        self.assertEqual(max(state['rates']), 0)       # The store's counter did not move in this run.


if __name__ == '__main__':
    unittest.main(verbosity=2)
