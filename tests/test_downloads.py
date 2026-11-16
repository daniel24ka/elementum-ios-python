"""Real peer transfers, persisted recovery and offline range playback."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'addon/resources/lib'))
from elementum_py.core import TorrentError
from elementum_py.downloads import Downloads
from elementum_py.engine import TorrentEngine
from test_protocols import Fixture


class DownloadsTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.fixture = Fixture()
        self.resolve = patch('elementum_py.engine.resolve_magnet', return_value=(
            self.fixture.meta, [('127.0.0.1', self.fixture.tcp.server_address[1])]))
        self.resolve.start()
        self.manager = Downloads(self.folder.name, free_space=lambda path: 10 * 1024 ** 3)
        self.uri = 'magnet:?xt=urn:btih:' + self.fixture.meta.info_hash.hex()

    def tearDown(self):
        self.manager.close()
        self.resolve.stop()
        self.fixture.close()
        self.folder.cleanup()

    def wait(self, identity, states=('completed', 'failed'), timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            job = self.manager.snapshot(identity)[0]
            if job['status'] in states:
                return job
            time.sleep(0.02)
        self.fail('Download did not reach expected state: ' + repr(job))

    def test_complete_then_offline_seek_and_integrity(self):
        identity = self.manager.add(self.uri, 'Saved fixture', peer_only=True)
        job = self.wait(identity)
        self.assertEqual(job['status'], 'completed', job.get('error'))
        self.assertEqual(job['completed_bytes'], len(self.fixture.payload))
        self.assertEqual(self.manager.add(self.uri), identity)
        self.assertNotIn('uri', json.dumps(job))
        saved = self.manager.playback(identity)
        offline = TorrentEngine(saved['profile'])
        try:
            with patch('elementum_py.engine.resolve_magnet', side_effect=AssertionError('Offline must not discover')):
                prepared = offline.prepare_cached(saved['info_hash'], saved['file_index'])
                request = Request(prepared['url'], headers={'Range': 'bytes=450000-520000'})
                with urlopen(request, timeout=3) as response:
                    self.assertEqual(response.status, 206)
                    self.assertEqual(response.read(), self.fixture.payload[450000:520001])
                self.assertIsNone(offline.active['pool'])
        finally:
            offline.stop()
        piece = Path(saved['profile']) / 'python-torrents' / saved['info_hash'] / 'pieces/0.piece'
        piece.write_bytes(b'x' * self.fixture.piece_length)
        with self.assertRaises(TorrentError):
            offline.prepare_cached(saved['info_hash'], saved['file_index'])
        self.assertIsNone(offline.active)

    def test_recheck_detects_saved_corruption_and_resume_repairs_it(self):
        identity = self.manager.add(self.uri, peer_only=True)
        self.assertEqual(self.wait(identity)['status'], 'completed')
        saved = self.manager.playback(identity)
        piece = Path(saved['profile']) / 'python-torrents' / saved['info_hash'] / 'pieces/3.piece'
        piece.write_bytes(b'corrupted')
        with patch('elementum_py.engine.resolve_magnet', side_effect=AssertionError('Recheck must stay offline')):
            self.manager.control(identity, 'recheck')
            self.assertEqual(self.wait(identity)['status'], 'failed')
        self.manager.control(identity, 'resume')
        self.assertEqual(self.wait(identity)['status'], 'completed')
        self.assertEqual(hashlib.sha1(piece.read_bytes()).digest(), self.fixture.meta.hashes[3])

    def test_low_storage_does_not_prevent_offline_recheck(self):
        identity = self.manager.add(self.uri)
        self.assertEqual(self.wait(identity)['status'], 'completed')
        self.manager.free_space = lambda path: 0
        self.manager.control(identity, 'recheck')
        self.assertEqual(self.wait(identity)['status'], 'completed')

    def test_storage_check_before_piece_transfer(self):
        self.manager.free_space = lambda path: 512 * 1024 ** 2
        identity = self.manager.add(self.uri)
        job = self.wait(identity)
        self.assertEqual(job['status'], 'failed')
        self.assertIn('storage', job['error'])
        self.assertEqual(list(Path(self.manager.folder(identity)).rglob('*.piece')), [])

    def test_pause_resume_recovery_and_bounded_parallelism(self):
        # Block actual peer piece transfer, then interrupt with the real engine cancellation.
        from elementum_py.peers import PeerPool
        original = PeerPool.fetch
        def slow(pool, index, **options):
            if pool.cancel.wait(0.15):
                raise TorrentError('Cancelled')
            return original(pool, index, **options)
        with patch.object(PeerPool, 'fetch', slow):
            identity = self.manager.add(self.uri)
            self.wait(identity, ('downloading',))
            self.manager.control(identity, 'pause')
            self.wait(identity, ('paused',))
            self.manager.close()
            self.manager = Downloads(self.folder.name, free_space=lambda path: 10 * 1024 ** 3)
            self.assertEqual(self.manager.snapshot(identity)[0]['status'], 'paused')
            self.manager.control(identity, 'resume')
            second = self.manager.add(self.uri + '&dn=second')
            self.wait(second, ('downloading', 'completed'))
            with self.manager.lock:
                self.assertLessEqual(len(self.manager.workers), 2)
            self.assertEqual(self.wait(identity)['status'], 'completed')
            self.assertEqual(self.wait(second)['status'], 'completed')

    def test_journal_restart_cancel_and_safe_removal(self):
        identity = self.manager.add(self.uri)
        self.manager.control(identity, 'cancel')
        self.wait(identity, ('cancelled',))
        self.manager.close()
        path = Path(self.manager.path)
        saved = json.loads(path.read_text())
        saved['jobs'][0]['status'] = 'downloading'
        path.write_text(json.dumps(saved))
        self.manager = Downloads(self.folder.name)
        self.assertEqual(self.manager.snapshot(identity)[0]['status'], 'paused')
        for bad in ('../', '..', 'f' * 24 + '/..'):
            with self.assertRaises(TorrentError):
                self.manager.remove(bad, True)
        self.manager.remove(identity, True)
        self.assertEqual(self.manager.snapshot(), [])

    def test_validation_secret_free_snapshot_and_metadata(self):
        self.manager.close()
        self.manager = Downloads(self.folder.name)
        with self.manager.changed:
            identity = self.manager.add(self.uri + '&tr=https://tracker.invalid/secret', media={'tmdb': 1, 'token': 'private'})
            self.manager.control(identity, 'pause')
        snapshot = json.dumps(self.manager.snapshot())
        self.assertNotIn('secret', snapshot)
        self.assertNotIn('private', snapshot)
        for uri in ('file:///etc/passwd', 'magnet:?x=1\n', None):
            with self.assertRaises(TorrentError):
                self.manager.add(uri)
        with self.assertRaises(TorrentError):
            self.manager.playback(identity)

    def test_old_service_worker_cannot_overwrite_journal_after_restart(self):
        self.manager._parallelism = lambda: 0
        identity = self.manager.add(self.uri)
        old = self.manager
        old.close()
        self.manager = Downloads(self.folder.name)
        self.assertEqual(self.manager.snapshot(identity)[0]['status'], 'paused')
        self.manager._set(self.manager.jobs[identity], title='Updated after restart')
        old._set(old.jobs[identity], title='Stale worker completion', status='completed')
        journal = json.loads(Path(self.manager.path).read_text())
        self.assertEqual(journal['jobs'][0]['title'], 'Updated after restart')
        self.assertEqual(journal['jobs'][0]['status'], 'paused')


if __name__ == '__main__':
    unittest.main()
