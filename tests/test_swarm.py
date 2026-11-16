"""Block-level swarm scheduling against scripted peers with real rates and faults.

The peers come from tools/swarm_bench.py: real TCP sockets on loopback, each
with its own transfer rate, bitfield and fault script.
"""

import hashlib
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'addon/resources/lib'))
sys.path.insert(0, str(ROOT / 'tools'))
import swarm_bench
import elementum_py.peers as peers_module
from elementum_py.core import Metainfo, PieceStore, TorrentError, bencode
from elementum_py.engine import SwarmReads, TorrentEngine
from elementum_py.peers import PeerPool

KIB, MIB = 1024, 1024 * 1024


class Scripted:
    """A torrent served by scripted peers; `content(index)` is the expected piece."""

    def __init__(self, peers, size=8 * MIB, piece=MIB, bitrate=MIB, dead=0):
        self.scenario = dict(size=size, piece=piece, bitrate=bitrate, peers=peers, dead=dead)
        self.seed = b'unit'
        self.meta = Metainfo(bencode(swarm_bench.build_metainfo(self.scenario, self.seed)))
        self.swarm = swarm_bench.Swarm(self.scenario, self.seed)
        self.addresses = [tuple(address) for address in self.swarm.serve(self.meta.info_hash)]

    def content(self, index):
        return self.swarm.content(index)

    def senders(self, index):
        return {block[1] for block in self.swarm.stats()['blocks'] if block[2] == index}

    def close(self):
        self.swarm.close()


def rates(count, rate, **options):
    return [dict(rate=rate, **options) for _ in range(count)]


class SwarmCase(unittest.TestCase):
    def start(self, peers, wait_unchoked=None, max_connections=20, ordered=False, **torrent):
        self.scripted = Scripted(peers, **torrent)
        self.addCleanup(self.scripted.close)
        if ordered:
            with patch.object(peers_module.random.SystemRandom, 'shuffle', lambda *args: None):
                self.pool = PeerPool(self.scripted.meta, self.scripted.addresses, max_connections=max_connections)
        else:
            self.pool = PeerPool(self.scripted.meta, self.scripted.addresses, max_connections=max_connections)
        self.addCleanup(self.pool.close)
        self.pool.prepare(timeout=5)
        wanted = wait_unchoked if wait_unchoked is not None else sum(not peer.get('never_unchoke') for peer in peers)
        self.wait(lambda: self.pool.stats()['unchoked_peers'] >= wanted, 'peers did not unchoke')
        return self.pool

    def wait(self, condition, message='condition was not reached', timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return
            time.sleep(.02)
        self.fail(message)


class SchedulingTests(SwarmCase):
    def test_every_unchoked_peer_shares_one_urgent_piece(self):
        pool = self.start(rates(8, 256 * KIB), size=4 * MIB, piece=2 * MIB)
        started = time.monotonic()
        self.assertEqual(pool.fetch(0), self.scripted.content(0))
        elapsed = time.monotonic() - started
        # One peer alone needs eight seconds for these two mebibytes.
        self.assertLess(elapsed, 4)
        self.assertGreaterEqual(len(self.scripted.senders(0)), 6)

    def test_stalled_peer_does_not_hold_an_urgent_piece(self):
        pool = self.start(rates(2, 400 * KIB) + rates(1, 400 * KIB, stall_after=256 * KIB), size=4 * MIB, piece=2 * MIB)
        started = time.monotonic()
        self.assertEqual(pool.fetch(0), self.scripted.content(0))
        self.assertEqual(pool.fetch(1), self.scripted.content(1))
        # Four mebibytes at 800 KiB/s take 5.1 s; the old engine waited 12-45 s for the silent peer.
        self.assertLess(time.monotonic() - started, 9)
        stats = pool.stats()
        self.assertGreaterEqual(stats['hedged_blocks'] + stats['snubbed_peers'], 1)

    def test_peer_without_the_piece_stays_connected(self):
        pool = self.start([dict(rate=2 * MIB, have=.5), dict(rate=2 * MIB)], size=8 * MIB, piece=MIB)
        self.assertEqual(pool.fetch(7), self.scripted.content(7))
        self.assertEqual(self.scripted.senders(7), {1})
        self.assertEqual(pool.stats()['connected_peers'], 2)
        self.assertEqual(pool.fetch(1), self.scripted.content(1))

    def test_corrupt_block_is_traced_to_its_sender_and_only_it_is_banned(self):
        pool = self.start(rates(2, 600 * KIB) + rates(1, 600 * KIB, corrupt=True), size=2 * MIB, piece=MIB)
        self.assertEqual(pool.fetch(0), self.scripted.content(0))
        self.wait(lambda: pool.stats()['connected_peers'] == 2, 'the corrupt peer was not dropped')
        stats = pool.stats()
        self.assertEqual(stats['banned_peers'], 1)
        self.assertIn(self.scripted.addresses[2], pool.swarm.banned)
        failures = stats['hash_failures']
        self.assertEqual(pool.fetch(1), self.scripted.content(1))
        self.assertEqual(pool.stats()['hash_failures'], failures)

    def test_only_corrupt_source_fails_instead_of_delivering_bad_bytes(self):
        pool = self.start(rates(1, 2 * MIB, corrupt=True), size=2 * MIB, piece=MIB)
        with self.assertRaisesRegex(TorrentError, 'SHA-1'):
            pool.fetch(0, timeout=5)

    def test_choked_peers_are_replaced_when_candidates_wait(self):
        with patch.object(peers_module, 'CHOKED_EVICTION', .4):
            pool = self.start(rates(4, MIB, never_unchoke=True) + rates(2, MIB), wait_unchoked=0,
                              max_connections=4, ordered=True, size=2 * MIB, piece=MIB)
            self.assertEqual(pool.stats()['unchoked_peers'], 0)
            self.assertEqual(pool.fetch(0, timeout=12), self.scripted.content(0))
            stats = pool.stats()
            self.assertGreaterEqual(stats['evicted_peers'], 1)
            self.assertLessEqual(stats['connected_peers'], 4)
        pool.close()
        self.assertFalse(any(worker.is_alive() for worker in pool.connectors))
        self.assertFalse(pool.maintainer.is_alive())

    def test_peers_without_any_wanted_piece_are_replaced(self):
        with patch.object(peers_module, 'USELESS_EVICTION', .4):
            pool = self.start(rates(4, MIB, have=0) + rates(2, MIB), wait_unchoked=4,
                              max_connections=4, ordered=True, size=2 * MIB, piece=MIB)
            self.assertEqual(pool.fetch(0, timeout=12), self.scripted.content(0))
            self.assertGreaterEqual(pool.stats()['evicted_peers'], 1)
            self.assertLessEqual(pool.stats()['connected_peers'], 4)
        pool.close()
        self.assertFalse(any(worker.is_alive() for worker in pool.connectors))

    def test_dropped_peer_that_delivered_data_is_dialed_again(self):
        pool = self.start(rates(1, 2 * MIB, drop_after=1.2), size=4 * MIB, piece=MIB)
        self.assertEqual(pool.fetch(0), self.scripted.content(0))
        self.wait(lambda: pool.stats()['connected_peers'] == 0, 'the scripted disconnect did not happen')
        self.assertEqual(pool.fetch(1, timeout=10), self.scripted.content(1))
        self.assertGreaterEqual(pool.stats()['redials'], 1)

    def test_dead_candidates_fail_fast_instead_of_waiting_for_the_timeout(self):
        scripted = Scripted([], dead=6, size=MIB, piece=MIB)
        self.addCleanup(scripted.close)
        pool = PeerPool(scripted.meta, scripted.addresses)
        self.addCleanup(pool.close)
        started = time.monotonic()
        with self.assertRaises(TorrentError):
            pool.prepare(timeout=15)
        self.assertLess(time.monotonic() - started, 8)

    def test_fast_extension_have_all_and_rejects(self):
        pool = self.start([dict(rate=2 * MIB, fast=True, reject_first=6), dict(rate=2 * MIB)], size=2 * MIB, piece=MIB)
        self.assertEqual(pool.fetch(0), self.scripted.content(0))
        self.assertEqual(pool.fetch(1), self.scripted.content(1))
        stats = pool.stats()
        self.assertEqual(stats['fast_extension_peers'], 1)
        self.assertEqual(stats['seeding_peers'], 2)
        self.assertEqual(stats['connected_peers'], 2)

    def test_idle_connections_outlive_the_old_ten_second_cutoff(self):
        # The previous reader dropped every peer that sent nothing for ten seconds.
        pool = self.start(rates(3, 2 * MIB), size=2 * MIB, piece=MIB)
        self.assertEqual(pool.fetch(0), self.scripted.content(0))
        time.sleep(11)
        self.assertEqual(pool.stats()['connected_peers'], 3)
        self.assertEqual(pool.fetch(1), self.scripted.content(1))

    def test_keepalive_is_sent_on_a_quiet_connection(self):
        with patch.object(peers_module, 'KEEPALIVE', .3):
            self.start(rates(1, MIB), size=MIB, piece=MIB)
            self.wait(lambda: self.scripted.swarm.keepalives >= 2, 'no keep-alive reached the peer', timeout=4)


class StoreIntegrationTests(SwarmCase):
    def store(self, pool, cache=32 * MIB):
        ready = threading.Event()
        ready.set()
        store = PieceStore(self.scripted.meta, max_cache_bytes=cache, scheduler=SwarmReads(pool, lambda: 30, ready),
                           allow_webseeds=False, prefetch_budget=lambda: store.prefetch_workers)
        self.addCleanup(store.close)
        return store

    def test_urgent_read_overtakes_read_ahead_on_a_single_peer(self):
        pool = self.start(rates(1, 256 * KIB), size=8 * MIB, piece=256 * KIB)
        store = self.store(pool)
        store.prefetch([1, 2, 3, 4])
        self.wait(lambda: len(pool.swarm.jobs) >= 3, 'read-ahead did not start')
        started = time.monotonic()
        self.assertEqual(store.piece(20), self.scripted.content(20))
        # Four read-ahead pieces alone need four seconds on this peer.
        self.assertLess(time.monotonic() - started, 2.6)

    def test_reader_waiting_on_read_ahead_promotes_it(self):
        pool = self.start(rates(2, 200 * KIB), size=8 * MIB, piece=256 * KIB)
        store = self.store(pool)
        store.prefetch([5, 6, 7])
        self.wait(lambda: 7 in pool.swarm.jobs, 'read-ahead did not start')
        self.assertFalse(pool.swarm.jobs[7].urgent)
        reader = threading.Thread(target=store.piece, args=(7,))
        reader.start()
        self.wait(lambda: 7 not in pool.swarm.jobs or pool.swarm.jobs[7].urgent, 'the awaited piece stayed read-ahead')
        reader.join(timeout=10)
        self.assertEqual(store.cache[7], self.scripted.content(7))

    def test_seek_abandons_started_read_ahead_and_refills_at_the_target(self):
        pool = self.start(rates(2, 200 * KIB), size=16 * MIB, piece=256 * KIB)
        store = self.store(pool)
        store.configure_stream(0)
        store.update_playback(16)
        first = store.read(0, 0, 16 * MIB)
        next(first)
        self.wait(lambda: any(index < 20 for index in pool.swarm.jobs), 'read-ahead did not start')
        seek = store.read(0, 10 * MIB, 6 * MIB)
        self.assertEqual(bytes(next(seek)), self.scripted.content(40))
        self.wait(lambda: pool.swarm.jobs and all(index >= 40 for index in pool.swarm.jobs),
                  'obsolete read-ahead kept its peers')
        first.close()
        seek.close()

    def test_engine_fills_a_start_buffer_and_knows_the_bitrate(self):
        self.scripted = Scripted(rates(6, MIB), size=24 * MIB, piece=256 * KIB, bitrate=512 * KIB)
        self.addCleanup(self.scripted.close)
        with tempfile.TemporaryDirectory() as profile:
            messages = []
            engine = TorrentEngine(profile, lambda key: {'download_storage': '1', 'dht_enabled': 'false'}.get(key, ''),
                                   lambda *args: messages.append(args))
            uri = 'magnet:?xt=urn:btih:' + self.scripted.meta.info_hash.hex()
            with patch('elementum_py.engine.resolve_magnet', return_value=(self.scripted.meta, self.scripted.addresses)):
                try:
                    prepared = engine.prepare(uri, peer_only=True)
                    report = engine.last_preparation
                    self.assertEqual(report['duration_seconds'], 48.0)
                    self.assertEqual(report['bitrate'], 512 * KIB)
                    buffered = report['start_buffer']
                    # Eight seconds of this half-mebibyte-per-second film, or an early
                    # start because six such peers are far faster than the video.
                    self.assertEqual(buffered['target_bytes'], 4 * MIB)
                    self.assertGreaterEqual(buffered['buffered_bytes'], MIB)
                    self.assertTrue(any('Buffering' in args[0] and '%' in args[0] for args in messages))
                    self.assertEqual(engine.reuse(uri)['url'], prepared['url'])
                    self.assertIsNone(engine.reuse(uri, 3))
                    self.assertIsNone(engine.reuse('magnet:?xt=urn:btih:' + '0' * 40))
                    stats = engine.stats()
                    self.assertEqual(stats['buffer']['pinned_pieces'], 2)
                    self.assertGreater(stats['swarm_rate'], 0)
                finally:
                    engine.stop()
                self.assertIsNone(engine.reuse(uri))


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.length = 16384
        self.payload = b''.join(hashlib.sha1(bytes([index])).digest() * 820 for index in range(200))[:200 * self.length]
        info = {b'name': b'film.mp4', b'length': len(self.payload), b'piece length': self.length,
                b'pieces': b''.join(hashlib.sha1(self.payload[start:start + self.length]).digest()
                                    for start in range(0, len(self.payload), self.length))}
        self.meta = Metainfo(bencode({b'info': info}))
        self.calls = []
        self.delay = .002

    def fetch(self, index):
        self.calls.append(index)
        time.sleep(self.delay)
        return self.payload[index * self.length:(index + 1) * self.length]

    def store(self, pieces):
        store = PieceStore(self.meta, max_cache_bytes=self.length * pieces, peer_fetcher=self.fetch, allow_webseeds=False)
        self.addCleanup(store.close)
        store.configure_stream(0)
        store.update_playback(100)
        return store

    def test_full_cache_never_evicts_read_ahead_before_it_is_read(self):
        store = self.store(12)
        received = bytearray()
        for chunk in store.read(0, 0, len(self.payload)):
            received += chunk
            time.sleep(.004)   # The player reads slower than the swarm delivers.
        self.assertEqual(bytes(received), self.payload)
        # Plain LRU order downloaded every piece almost four times here.
        self.assertEqual(sorted(self.calls), list(range(200)))
        self.assertLessEqual(len(store.cache), 12)

    def test_head_and_tail_stay_cached_while_the_middle_rolls(self):
        store = self.store(20)
        self.assertEqual(store.pinned, frozenset((0, 199)))
        store.piece(199)
        for chunk in store.read(0, 0, 150 * self.length):
            pass
        self.assertIn(0, store.cache)
        self.assertIn(199, store.cache)
        self.assertLessEqual(len(store.cache), 20)
        self.assertEqual(self.calls.count(0), 1)
        self.assertEqual(self.calls.count(199), 1)

    def test_small_cache_does_not_reserve_slots_for_head_and_tail(self):
        self.assertEqual(self.store(12).pinned, frozenset())

    def test_parked_reader_does_not_pull_the_window_back(self):
        store = self.store(12)
        parked = store.read(0, 0, len(self.payload))
        next(parked)
        with patch.object(PieceStore, 'PARKED_AFTER', .05):
            time.sleep(.1)     # Kodi keeps the old connection open without reading it.
            active = store.read(0, 100 * self.length, 50 * self.length)
            next(active)
            generation = store.window_generation
            active.close()
            self.assertEqual(store.window_generation, generation)
            self.assertTrue(all(index >= 100 for index in store.window))
        next(parked)           # Kodi swapped back: the reader that moves owns the window again.
        self.assertTrue(all(index < 20 for index in store.window))
        parked.close()

    def test_rate_counts_parallel_downloads_once(self):
        self.delay = .4
        store = PieceStore(self.meta, max_cache_bytes=self.length * 64, peer_fetcher=self.fetch, allow_webseeds=False)
        self.addCleanup(store.close)
        started = time.monotonic()
        for round_number in range(3):
            workers = [threading.Thread(target=store.piece, args=(round_number * 8 + index,)) for index in range(8)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
        elapsed = time.monotonic() - started
        true_rate = 24 * self.length / elapsed
        # Summing each piece's own duration reports one stream's rate: an eighth of this.
        self.assertGreater(store.rate(), true_rate * .7)
        self.assertLess(store.rate(), true_rate * 1.3)

    def test_heartbeat_read_reports_a_wait_and_then_the_exact_bytes(self):
        release = threading.Event()

        def fetch(index):
            if index == 1:
                release.wait(5)
            return self.payload[index * self.length:(index + 1) * self.length]

        store = PieceStore(self.meta, max_cache_bytes=self.length * 8, peer_fetcher=fetch, allow_webseeds=False)
        self.addCleanup(store.close)
        reader = store.read(0, 0, 3 * self.length, heartbeat=.03)
        chunks = [bytes(next(reader))]
        self.assertEqual(next(reader), b'')
        self.assertEqual(next(reader), b'')
        release.set()
        chunks += [bytes(chunk) for chunk in reader if len(chunk)]
        self.assertEqual(b''.join(chunks), self.payload[:3 * self.length])

    def test_failed_piece_is_remembered_for_diagnostics_until_it_succeeds(self):
        healthy = []

        def fetch(index):
            if not healthy:
                raise TorrentError('Peers timed out while sending this part of the video')
            return self.payload[index * self.length:(index + 1) * self.length]

        store = PieceStore(self.meta, peer_fetcher=fetch, allow_webseeds=False)
        self.addCleanup(store.close)
        with self.assertRaises(TorrentError):
            store.piece(3)
        self.assertIn('timed out', store.failures[3])
        healthy.append(True)
        store.piece(3)
        self.assertNotIn(3, store.failures)


class Stub:
    """A peer without a socket: scheduling decisions can be read from what it was sent."""

    def __init__(self, swarm, rate, name, depth=32):
        self.owner, self.rate, self.depth, self.address = swarm, float(rate), depth, (name, 1)
        self.closed = self.detached = self.choked = self.snubbed = self.suspect = self.fast = False
        self.known = self.complete = True
        self.bits, self.allowed_fast, self.outstanding, self.stale = None, set(), {}, {}
        self.queued_bytes = self.sample_bytes = self.rejects = self.pieces = 0
        self.bytes_received = self.wasted_bytes = 0
        self.blocks, self.request_limit = 1, 250
        self.sample_time = self.last_block = self.busy_since = self.useless_since = self.snubbed_since = 0.0
        self.quiet_since, self.chances, self.replies = 0.0, 0, 0
        self.new_peers, self.pex_flags, self.last_error = [], {}, ''
        self.last_sent = peers_module.clock()
        self.sent = []
        swarm.attach(self)

    has = peers_module.PeerSession.has
    estimate = peers_module.PeerSession.estimate
    timeout = peers_module.PeerSession.timeout

    def _send(self, data):
        for start in range(0, len(data), 17):
            kind, index, begin, length = peers_module._REQUEST.unpack_from(data, start)[1:]
            self.sent.append(('request' if kind == 6 else 'cancel', index, begin // peers_module.BLOCK))

    def close(self, reason=''):
        self.closed = True
        self.owner.detach(self)

    def join(self, timeout=1):
        pass

    def requested(self, index):
        return sorted(block for kind, piece, block in self.sent if kind == 'request' and piece == index)

    def cancelled(self, index):
        return sorted(block for kind, piece, block in self.sent if kind == 'cancel' and piece == index)


class SchedulerCase(unittest.TestCase):
    """A swarm of stub peers with exact, clock-independent states."""
    DONE = bytes([2])

    def setUp(self):
        payload = bytes(64 * 16384 * 8)
        info = {b'name': b'film.mkv', b'length': len(payload), b'piece length': 64 * 16384,
                b'pieces': hashlib.sha1(payload[:64 * 16384]).digest() * 8}
        self.meta = Metainfo(bencode({b'info': info}))
        self.swarm = peers_module.Swarm(self.meta)
        self.now = peers_module.clock()

    def job(self, index, urgent, wanted=64):
        """A piece whose first `wanted` blocks are still to be downloaded."""
        job = peers_module.PieceJob(self.meta, index, urgent)
        job.state[wanted:] = self.DONE * (64 - wanted)
        job.done = 64 - wanted
        self.swarm.jobs[index] = job
        self.swarm._reorder()
        return job

    def assign(self, *peers):
        outbox = {}
        with self.swarm.lock:
            self.swarm._assign(list(peers) or self.swarm.peers, self.now, outbox)
        self.swarm._flush(outbox)

    def hold(self, peer, job, block, arrives_in):
        """Record that `peer` was asked for `block` and should deliver it at now + arrives_in."""
        key = (job.index, block * peers_module.BLOCK)
        peer.outstanding[key] = [peers_module.BLOCK, self.now - 1, self.now + arrives_in]
        peer.queued_bytes += peers_module.BLOCK
        job.holders.setdefault(block, {})[peer] = self.now + arrives_in
        job.state[block] = 1
        job.cursor = 64


class SchedulerDecisionTests(SchedulerCase):
    """The swarm's choices, read from what each stub peer was sent."""

    def test_urgent_blocks_replace_read_ahead_at_the_far_end_of_a_full_pipeline(self):
        peer = Stub(self.swarm, 16 * 16384, 'a', depth=32)      # Two seconds of work when full.
        self.job(0, urgent=False)
        self.assign()
        self.assertEqual(peer.requested(0), list(range(32)))
        self.job(5, urgent=True, wanted=4)
        self.assign()
        self.assertEqual(peer.requested(5), [0, 1, 2, 3])
        # Exactly four read-ahead requests gave way, and they were the last in line.
        self.assertEqual(peer.cancelled(0), [28, 29, 30, 31])
        self.assertEqual(len(peer.outstanding), 32)

    def test_block_held_only_by_quiet_peers_is_requested_again_for_any_piece(self):
        quiet, snubbed, healthy = (Stub(self.swarm, 16 * 16384, name) for name in 'abc')
        quiet.suspect, snubbed.snubbed = True, True
        job = self.job(0, urgent=False, wanted=1)
        self.hold(quiet, job, 0, arrives_in=.2)
        self.hold(snubbed, job, 0, arrives_in=.1)
        self.assign(healthy)
        self.assertEqual(healthy.requested(0), [0])
        self.assertEqual(quiet.cancelled(0) + snubbed.cancelled(0), [])
        # With a healthy holder, read-ahead is not duplicated again.
        other = Stub(self.swarm, 16 * 16384, 'd')
        job.hedge_after = 0
        self.assign(other)
        self.assertEqual(other.requested(0), [])

    def test_slow_peer_leaves_an_urgent_piece_to_faster_peers(self):
        fast = Stub(self.swarm, 64 * 16384, 'fast', depth=64)
        slow = Stub(self.swarm, 16384 // 2, 'slow', depth=4)
        self.job(0, urgent=True, wanted=4)
        self.assign(slow, fast)
        # The fast peer needs a sixteenth of a second for all four blocks;
        # the slow one needs two seconds for a single block.
        self.assertEqual(fast.requested(0), [0, 1, 2, 3])
        self.assertEqual(slow.requested(0), [])

    def test_block_deep_in_a_healthy_pipeline_is_taken_over_not_duplicated(self):
        busy, free = Stub(self.swarm, 16 * 16384, 'busy'), Stub(self.swarm, 16 * 16384, 'free')
        job = self.job(0, urgent=True, wanted=1)
        self.hold(busy, job, 0, arrives_in=3)
        self.assign(free)
        self.assertEqual(free.requested(0), [0])
        self.assertEqual(busy.cancelled(0), [0])
        self.assertEqual(list(job.holders[0]), [free])
        self.assertEqual(self.swarm.counters['stolen_blocks'], 1)

    def test_overdue_urgent_block_is_requested_a_second_time_only_once(self):
        late, second, third = (Stub(self.swarm, 16 * 16384, name) for name in 'abc')
        job = self.job(0, urgent=True, wanted=1)
        self.hold(late, job, 0, arrives_in=-1)
        self.assign(second)
        self.assertEqual(second.requested(0), [0])
        self.assertEqual(late.cancelled(0), [])          # It may still arrive from the first peer.
        job.hedge_after = 0
        self.assign(third)
        self.assertEqual(third.requested(0), [])
        self.assertEqual(self.swarm.counters['hedged_blocks'], 1)

    def test_peer_that_went_quiet_gets_no_new_work_until_it_delivers(self):
        quiet, healthy = Stub(self.swarm, 16 * 16384, 'quiet'), Stub(self.swarm, 16 * 16384, 'healthy')
        job = self.job(0, urgent=False, wanted=2)
        self.hold(quiet, job, 0, arrives_in=-2)
        job.cursor = 1
        quiet.busy_since = self.now - 3
        self.swarm.last_tick = 0
        self.swarm.tick(self.now)
        self.assertTrue(quiet.suspect)
        self.assertEqual(quiet.requested(0), [])
        self.assertEqual(healthy.requested(0), [0, 1])   # The quiet peer's block is rescued too.
        self.swarm.block(quiet, 0, 0, bytes(peers_module.BLOCK))
        self.assertFalse(quiet.suspect)

    def test_awaited_block_of_a_quiet_peer_moves_to_the_front_of_a_full_pipeline(self):
        quiet = Stub(self.swarm, 16 * 16384, 'quiet')
        healthy = Stub(self.swarm, 16 * 16384, 'healthy', depth=32)
        self.job(3, urgent=False)
        self.assign(healthy)                              # Its pipeline is full of read-ahead.
        self.assertEqual(len(healthy.outstanding), 32)
        urgent = self.job(0, urgent=True, wanted=1)
        self.hold(quiet, urgent, 0, arrives_in=.1)
        quiet.suspect = True
        self.assign()
        self.assertEqual(healthy.requested(0), [0])
        self.assertEqual(healthy.cancelled(3), [31])      # One far read-ahead request gave way.
        self.assertEqual(quiet.cancelled(0), [])
        self.assertIn((0, 0), quiet.stale)                # A late copy from the quiet peer is still welcome.

    def test_read_ahead_follows_the_reader_after_a_seek(self):
        Stub(self.swarm, 16 * 16384, 'a')
        for index in (1, 2, 6, 7):
            self.job(index, urgent=False)
        self.swarm.set_focus(6)
        self.assertEqual([job.index for job in self.swarm.order], [6, 7, 1, 2])
        self.swarm.jobs[2].urgent = True
        self.swarm._reorder()
        self.assertEqual([job.index for job in self.swarm.order], [2, 6, 7, 1])
        self.swarm.demote(2)
        self.assertEqual([job.index for job in self.swarm.order], [6, 7, 1, 2])


if __name__ == '__main__':
    unittest.main(verbosity=2)
