"""A stopped, restarted or updated service gives its port back first, and the next instance waits for it.

Seen on an iPhone with 0.3.4~dev1: the add-on was switched off and on right after
playback. The old instance was still closing its swarm, Kodi ended it after five
seconds before it had closed its listener, and the new instance died with
"Address already in use" until Kodi itself was restarted.

The second half holds one test for each defect that an independent review then
confirmed in the first version of that fix.
"""

import concurrent.futures
import ctypes
import errno
import hashlib
import http.client
import importlib.util
import logging
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'addon/resources/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import elementum_py.core as core
import elementum_py.peers as peers
import elementum_py.streaming as streaming
from elementum_py.backend import Backend, BackendServer
from elementum_py.catalog import Catalog
from elementum_py.core import Metainfo, PieceStore, TorrentError, Workers, bencode, run_all
from elementum_py.discovery import end_executor
from elementum_py.engine import STOP_PATIENCE, TorrentEngine
from elementum_py.streaming import StreamServer, listener_alive, retire_listener
from test_backend import FakeEngine, FakeKodi, catalog_transport
import test_upgrades


def busy():
    return OSError(errno.EADDRINUSE, 'Address already in use')


def force_end(thread):
    """What Kodi does to a service that overstays: SystemExit, once, in a thread."""
    ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(thread.ident), ctypes.py_object(SystemExit))


def wait_until(condition, seconds=5.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(.02)
    return condition()


def service_module(clock):
    """kodi_service.py with stub Kodi modules and a clock the test moves."""
    stubs = {'xbmc': types.SimpleNamespace(), 'xbmcaddon': types.SimpleNamespace(),
             'xbmcgui': types.SimpleNamespace(), 'xbmcvfs': types.SimpleNamespace()}
    path = Path(sys.modules['elementum_py.backend'].__file__).with_name('kodi_service.py')
    spec = importlib.util.spec_from_file_location('elementum_py._restart_test', path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, stubs):
        spec.loader.exec_module(module)
    module.time = types.SimpleNamespace(monotonic=lambda: clock[0])
    return module


def film(pieces=4, length=16384, extra=None):
    payload = b''.join(hashlib.sha1(bytes([index % 256, index // 256])).digest() * 820
                       for index in range(pieces))[:pieces * length]
    info = {b'name': b'film.mkv', b'length': len(payload), b'piece length': length,
            b'pieces': b''.join(hashlib.sha1(payload[start:start + length]).digest()
                                for start in range(0, len(payload), length))}
    torrent = {b'info': info}
    torrent.update(extra or {})
    return Metainfo(bencode(torrent)), payload


def fake_session(engine, fail=None):
    """An active session whose parts record how they are closed; ``fail`` is ended by force."""
    seen = []

    def step(name, value=None):
        seen.append((name, value))
        if name == fail:
            raise SystemExit
    engine.active = {'cancel': threading.Event(),
                     'pool': types.SimpleNamespace(shut=lambda: step('pool.shut'),
                                                   close=lambda patience=10: step('pool.close', patience)),
                     'store': types.SimpleNamespace(close=lambda wait=True: step('store.close', wait)),
                     'stream': types.SimpleNamespace(close=lambda drop=False: step('stream.close', drop))}
    return seen


class BindTests(unittest.TestCase):
    def setUp(self):
        self.clock = [0.0]
        self.module = service_module(self.clock)
        self.waits, self.told, self.attempts = [], [], 0
        self.abort_after = None

    def wait(self, seconds):
        self.waits.append(seconds)
        self.clock[0] += seconds
        return self.abort_after is not None and len(self.waits) >= self.abort_after

    def factory(self, busy_times, error=busy):
        def make():
            self.attempts += 1
            if self.attempts <= busy_times:
                raise error()
            return 'listener'
        return make

    def test_busy_port_is_retried_until_the_previous_instance_lets_go(self):
        result = self.module.bind_patiently(self.factory(6), self.wait, 20.0, lambda: self.told.append(1))
        self.assertEqual(result, 'listener')
        self.assertEqual(self.waits, [0.5] * 6)      # Three seconds: nobody has to be told.
        self.assertEqual(self.told, [])

    def test_user_is_told_once_and_the_attempts_go_on(self):
        result = self.module.bind_patiently(self.factory(46), self.wait, 20.0, lambda: self.told.append(1))
        self.assertEqual(result, 'listener')         # The port came back after 50 seconds.
        self.assertEqual(self.told, [1])
        self.assertEqual(self.waits, [0.5] * 40 + [5.0] * 6)

    def test_kodi_stopping_the_service_ends_the_wait(self):
        self.abort_after = 3
        self.assertIsNone(self.module.bind_patiently(self.factory(10 ** 6), self.wait, 20.0, lambda: self.told.append(1)))
        self.assertEqual(self.attempts, 3)

    def test_another_error_is_not_retried(self):
        with self.assertRaises(PermissionError):
            self.module.bind_patiently(self.factory(5, lambda: PermissionError(errno.EACCES, 'denied')),
                                       self.wait, 20.0, lambda: self.told.append(1))
        self.assertEqual((self.attempts, self.waits), (1, []))

    def test_optional_listener_gives_up_after_its_patience(self):
        with self.assertRaises(OSError) as raised:
            self.module.bind_patiently(self.factory(10 ** 6), self.wait, 5.0)
        self.assertEqual(raised.exception.errno, errno.EADDRINUSE)
        self.assertEqual(self.waits, [0.5] * 10)


class BackendCase(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.kodi = FakeKodi()
        self.engine = FakeEngine()
        self.backend = Backend(self.folder.name, self.kodi, port=0, engine=self.engine,
                               catalog=Catalog(self.folder.name + '/catalog', transport=catalog_transport))
        self.addCleanup(self.backend.close, 0)


class ReleaseTests(BackendCase):
    def test_port_is_free_before_downloads_and_playback_are_stopped(self):
        server = BackendServer(self.backend)
        port, seen = self.backend.port, {}
        self.assertTrue(listener_alive(port))

        def stop(patience=None, final=False):
            seen.setdefault('port_open_during_stop', listener_alive(port))
            seen.update(patience=patience, final=final)
        self.engine.stop = stop
        close_downloads = self.backend.downloads.close

        def downloads_close(patience=10):
            seen.setdefault('port_open_during_downloads', listener_alive(port))
            seen.setdefault('downloads_patience', []).append(patience)
            close_downloads(patience)
        self.backend.downloads.close = downloads_close
        server.close()
        self.assertEqual(seen['port_open_during_downloads'], False)
        self.assertEqual(seen['port_open_during_stop'], False)
        self.assertEqual((seen['patience'], seen['final']), (0, True))   # Told at once; no later session.
        self.assertEqual(seen['downloads_patience'][0], 0)               # Saved and interrupted before any wait.
        self.assertLessEqual(seen['downloads_patience'][-1], 1.5)        # Inside Kodi's five seconds.
        self.assertTrue(self.backend.shutdown.is_set())

    def test_release_is_repeatable_and_the_watchdog_leaves_a_released_port_alone(self):
        server = BackendServer(self.backend)
        port = self.backend.port
        server.release()
        server.release()                                   # After an interrupted shutdown.
        self.assertFalse(listener_alive(port))
        self.assertFalse(server.ensure_listening())
        self.assertEqual(server.httpd.socket.fileno(), -1)   # Still closed; the port number may already serve another test.
        self.assertEqual(server.rebuilt, 0)
        server.close()

    def test_next_instance_binds_the_port_while_the_old_one_is_still_closing(self):
        old = BackendServer(self.backend)
        port = self.backend.port
        listener = old.httpd.socket
        closing, finish = threading.Event(), threading.Event()

        def slow_stop(patience=None, final=False):
            closing.set()
            finish.wait(10)                                # A swarm that takes its time.
        self.engine.stop = slow_stop
        closer = threading.Thread(target=old.close, daemon=True)
        closer.start()
        self.assertTrue(closing.wait(10))
        # Windows lets a server bind a port that still listens, so the bind of the
        # new instance alone proves nothing there. A plain socket is refused on
        # every platform while the old listener exists.
        self.assertEqual(listener.fileno(), -1)
        probe = socket.socket()
        try:
            probe.bind(('127.0.0.1', port))
        finally:
            probe.close()
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        backend = Backend(folder.name, FakeKodi(), port=port, engine=FakeEngine(),
                          catalog=Catalog(folder.name + '/catalog', transport=catalog_transport))
        new = BackendServer(backend)                       # Must not raise "Address already in use".
        self.addCleanup(new.close)
        self.assertEqual(backend.port, port)
        self.assertTrue(listener_alive(port))
        finish.set()
        closer.join(10)
        self.assertFalse(closer.is_alive())
        self.assertTrue(listener_alive(port))              # The old instance did not take the new listener down.

    def test_threads_end_when_the_listener_never_opened(self):
        calls = []
        self.engine.stop = lambda patience=None, final=False: calls.append((patience, final))
        scheduler = self.backend.downloads.scheduler
        self.assertTrue(scheduler.is_alive())
        self.backend.close()
        scheduler.join(3)
        self.assertFalse(scheduler.is_alive())             # Kodi waits for every thread of a finished script.
        self.assertEqual(calls, [(0, True)])

    def test_one_failing_step_does_not_keep_the_others_running(self):
        calls = []
        self.engine.stop = lambda patience=None, final=False: calls.append('engine')
        close_downloads = self.backend.downloads.close

        def broken(patience=10):
            close_downloads(patience)
            raise OSError('journal could not be written')
        self.backend.downloads.close = broken
        self.backend.web = types.SimpleNamespace(close=lambda: calls.append('web'))
        logged = []
        self.kodi.log_error = lambda kind, message: logged.append(kind)
        self.backend.close()
        self.assertEqual(calls, ['engine', 'web'])
        self.assertEqual(logged, ['OSError', 'OSError'])   # Both passes over the downloads; never silent.


class StreamListenerTests(unittest.TestCase):
    def test_watchdog_does_not_reopen_the_stream_of_a_session_that_ended(self):
        meta, payload = film()
        store = PieceStore(meta, max_cache_bytes=len(payload), allow_webseeds=False,
                           peer_fetcher=lambda index: payload[index * 16384:(index + 1) * 16384])
        self.addCleanup(store.close)
        stream = StreamServer(store, 0)
        port = stream.port
        self.assertTrue(listener_alive(port))
        stream.close()
        self.assertFalse(listener_alive(port))
        self.assertFalse(stream.ensure_listening())        # The service's check arriving a moment later.
        self.assertFalse(listener_alive(port))
        self.assertEqual(stream.stats()['listener_rebuilt'], 0)
        stream.close()                                     # Closing twice stays harmless.

    def answers(self, *outcomes):
        calls = []

        def connect(address, timeout=None):
            calls.append(address)
            outcome = outcomes[len(calls) - 1]
            if isinstance(outcome, Exception):
                raise outcome
            return types.SimpleNamespace(close=lambda: None)
        return calls, patch.object(streaming.socket, 'create_connection', connect)

    def test_refused_connection_is_decided_at_once(self):
        calls, patched = self.answers(ConnectionRefusedError())
        with patched:
            self.assertFalse(listener_alive(1))
        self.assertEqual(len(calls), 1)

    def test_one_slow_accept_does_not_condemn_a_healthy_listener(self):
        calls, patched = self.answers(socket.timeout(), None)
        with patched:
            self.assertTrue(listener_alive(1))
        self.assertEqual(len(calls), 2)
        calls, patched = self.answers(socket.timeout(), socket.timeout())
        with patched:
            self.assertFalse(listener_alive(1))
        self.assertEqual(len(calls), 2)


class BoundedStopTests(unittest.TestCase):
    def silent_peer(self):
        silent = socket.socket()                           # Accepts the connection, never answers the handshake.
        silent.bind(('127.0.0.1', 0))
        silent.listen(8)
        self.addCleanup(silent.close)
        return silent.getsockname()

    def test_pool_close_does_not_wait_for_a_dial_that_is_still_timing_out(self):
        meta, _ = film()
        with patch.object(peers, 'HANDSHAKE_TIMEOUT', 2.0):
            pool = peers.PeerPool(meta, [self.silent_peer()])
            pool._start_workers()
            self.assertTrue(wait_until(lambda: pool.connecting == 1))
            workers = list(pool.connectors)
            started = time.monotonic()
            pool.close(patience=0.2)
            self.assertLess(time.monotonic() - started, 1.0)
            for worker in workers:                         # The dial ends by itself.
                worker.join(6)
            self.assertFalse(any(worker.is_alive() for worker in workers))
            self.assertEqual(pool.connecting, 0)

    def test_dial_that_completes_after_the_pool_closed_is_closed_and_not_kept(self):
        meta, _ = film()
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(4)
        self.addCleanup(listener.close)
        seen = {}

        def late_peer():
            channel, _ = listener.accept()
            with channel:
                channel.settimeout(8)
                hello = b''
                while len(hello) < 68:
                    hello += channel.recv(68 - len(hello))
                time.sleep(1.2)                            # Longer than the pool's patience below.
                channel.sendall(hello[:28] + meta.info_hash + b'-XX0001-abcdefghijkl')
                try:
                    while channel.recv(4096):
                        pass
                    seen['closed_by_client'] = True
                except OSError:
                    seen['closed_by_client'] = True
        thread = threading.Thread(target=late_peer, daemon=True)
        thread.start()
        pool = peers.PeerPool(meta, [listener.getsockname()])
        pool._start_workers()
        self.assertTrue(wait_until(lambda: pool.connecting == 1))
        workers = list(pool.connectors)
        started = time.monotonic()
        pool.close(patience=0.2)
        self.assertLess(time.monotonic() - started, 1.0)   # Returned before the handshake completed.
        for worker in workers:
            worker.join(6)
        thread.join(6)
        self.assertTrue(seen.get('closed_by_client'))      # Not left attached to a stopped swarm.
        self.assertEqual(pool.connecting, 0)
        self.assertFalse([peer for peer in pool.sessions if not peer.closed])

    def session(self, engine, fail=None):
        return fake_session(engine, fail)

    def test_stop_tells_everything_first_and_waits_last(self):
        with tempfile.TemporaryDirectory() as profile:
            engine = TorrentEngine(profile, lambda key: '', lambda *message: None)
            seen = self.session(engine)
            cancel = engine.active['cancel']
            engine.stop()
            self.assertIsNone(engine.active)
            self.assertTrue(cancel.is_set())
            names = [name for name, _ in seen]
            self.assertEqual(names, ['pool.shut', 'store.close', 'stream.close', 'store.close', 'pool.close'])
            self.assertEqual([value for name, value in seen if name == 'store.close'], [False, True])
            self.assertEqual(dict(seen)['stream.close'], False)       # Open responses run on: a source switch.
            self.assertLess(dict(seen)['pool.close'], STOP_PATIENCE + .001)
            self.assertGreater(dict(seen)['pool.close'], STOP_PATIENCE - 1)

    def test_stopping_service_waits_for_nothing_and_drops_the_open_responses(self):
        with tempfile.TemporaryDirectory() as profile:
            engine = TorrentEngine(profile, lambda key: '', lambda *message: None)
            seen = self.session(engine)
            engine.stop(0.4)
            self.assertEqual([name for name, _ in seen], ['pool.shut', 'store.close', 'stream.close', 'pool.close'])
            self.assertEqual(dict(seen)['store.close'], False)
            self.assertEqual(dict(seen)['stream.close'], True)
            self.assertLess(dict(seen)['pool.close'], 0.401)


class ServiceShutdownTests(test_upgrades.ServiceLoopCase):
    def recording(self, calls, busy_times=0, close=None):
        attempts = []

        def factory(*args):
            attempts.append(1)
            if len(attempts) <= busy_times:
                raise busy()

            def closing():
                calls.append('close')
                if close:
                    close()
            return types.SimpleNamespace(release=lambda: calls.append('release'), close=closing)
        return factory

    def test_port_is_given_back_before_anything_else(self):
        calls = []
        state = self.exercise('stopped', server=self.recording(calls))
        self.assertEqual(calls, ['release', 'close'])
        self.assertEqual(state['backend_closed'], 0)       # The close above did it all; no second pass.

    def test_service_starts_once_the_previous_instance_frees_the_port(self):
        calls = []
        state = self.exercise('stopped', server=self.recording(calls, busy_times=3))
        self.assertEqual(state['stops'], 1)                # The loop ran and handled the stopped film.
        self.assertEqual(calls, ['release', 'close'])

    def test_interrupted_shutdown_still_frees_the_port_and_tells_everything_to_stop(self):
        calls = []

        def killed():
            raise SystemExit                               # What Kodi raises in a script that overstays.
        with self.assertRaises(SystemExit):
            self.exercise('stopped', server=self.recording(calls, close=killed))
        self.assertEqual(calls, ['release', 'close', 'release'])
        self.assertEqual(self.last_state['backend_closed'], 1)   # The last pass: everything told, nothing waited for.

    def test_service_stopped_while_waiting_for_the_port_closes_its_threads(self):
        calls = []
        state = self.exercise('stopped', server=self.recording(calls, busy_times=10 ** 6), check=False)
        self.assertEqual(calls, [])                        # No listener ever existed.
        self.assertEqual(state['backend_closed'], 1)
        self.assertEqual([kind for kind, _ in state['errors']], ['Startup'])   # Told once after twenty seconds.

    def test_startup_failure_other_than_a_busy_port_is_reported_and_cleaned_up(self):
        def factory(*args):
            raise PermissionError(errno.EACCES, 'denied')
        state = self.exercise('stopped', server=factory, check=False)
        self.assertEqual([kind for kind, _ in state['errors']], ['PermissionError'])
        self.assertEqual(state['backend_closed'], 1)


class ForcedEndTests(BackendCase):
    """Kodi's forced end may land in any step; the steps after it still have to run."""

    def test_run_all_finishes_the_steps_and_raises_the_interruption_afterwards(self):
        calls, failures = [], []

        def interrupted():
            calls.append('interrupted')
            raise SystemExit

        def broken():
            calls.append('broken')
            raise ValueError('ordinary')
        with self.assertRaises(SystemExit):
            run_all([interrupted, broken, lambda: calls.append('last')], failed=failures.append)
        self.assertEqual(calls, ['interrupted', 'broken', 'last'])
        self.assertEqual([type(error) for error in failures], [ValueError])

    def test_forced_end_inside_the_downloads_step_still_stops_playback_and_the_dashboard(self):
        calls = []
        self.engine.stop = lambda patience=None, final=False: calls.append('engine')
        self.backend.web = types.SimpleNamespace(close=lambda: calls.append('web'))
        close_downloads = self.backend.downloads.close
        state = {'first': True}

        def interrupted(patience=10):
            close_downloads(patience)
            if state.pop('first', False):
                raise SystemExit
        self.backend.downloads.close = interrupted
        with self.assertRaises(SystemExit):
            self.backend.close()
        self.assertEqual(calls, ['engine', 'web'])

    def test_forced_end_inside_the_pool_still_closes_store_and_stream(self):
        with tempfile.TemporaryDirectory() as profile:
            engine = TorrentEngine(profile, lambda key: '', lambda *message: None)
            seen = fake_session(engine, fail='pool.shut')
            with self.assertRaises(SystemExit):
                engine.stop(0)
            self.assertEqual([name for name, _ in seen], ['pool.shut', 'store.close', 'stream.close', 'pool.close'])

    def test_server_close_stops_the_backend_even_when_the_port_could_not_be_released(self):
        server = BackendServer(self.backend)
        self.addCleanup(retire_listener, server.httpd, server.thread)
        calls = []
        self.engine.stop = lambda patience=None, final=False: calls.append('engine')

        def broken():
            raise RuntimeError("can't start new thread")
        server.release = broken
        with self.assertRaises(RuntimeError):
            server.close()
        self.assertEqual(calls, ['engine'])

    def test_listener_whose_accept_thread_never_started_is_retired_at_once(self):
        class Quiet(streaming.ThreadingHTTPServer):
            pass
        httpd = Quiet(('127.0.0.1', 0), streaming.BaseHTTPRequestHandler)
        never = threading.Thread(target=httpd.serve_forever, daemon=True)     # Not started.
        started = time.monotonic()
        retire_listener(httpd, never)                      # Used to wait two seconds, then raise.
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertEqual(httpd.socket.fileno(), -1)
        # No helper is left waiting for a loop that never ran.
        self.assertFalse([thread for thread in threading.enumerate()
                          if getattr(thread, '_target', None) == httpd.shutdown])

    def test_rebuild_that_cannot_start_its_thread_leaves_no_unserved_port(self):
        server = BackendServer(self.backend)
        self.addCleanup(server.close)
        port = self.backend.port
        hook, threading.excepthook = threading.excepthook, lambda arguments: None
        self.addCleanup(setattr, threading, 'excepthook', hook)
        server.httpd.socket.close()                        # What a suspended iOS app finds on resume.
        # The loop dies on Windows and keeps spinning where poll() is used; both are handled.
        wait_until(lambda: not server.thread.is_alive(), 1.0)
        real = threading.Thread

        class Failing(real):
            def start(self):
                if getattr(self._target, '__name__', '') == 'serve_forever':
                    raise RuntimeError("can't start new thread")
                real.start(self)
        with patch.object(streaming.threading, 'Thread', Failing):
            self.assertFalse(server.ensure_listening())
        self.assertIsNone(server.httpd)
        probe = socket.socket()                            # Nothing bound: a later check can repair it.
        try:
            probe.bind(('127.0.0.1', port))
        finally:
            probe.close()
        self.assertTrue(wait_until(server.ensure_listening, 6))
        self.assertTrue(listener_alive(port))


class SelfEndingThreadTests(unittest.TestCase):
    def test_idle_read_ahead_threads_leave_when_the_store_is_cancelled_without_a_close(self):
        stopped = threading.Event()
        workers = Workers(4, stopped)
        tasks = [workers.submit(time.sleep, .05) for _ in range(8)]
        for task in tasks:
            task.result(timeout=5)
        threads = list(workers.threads)
        self.assertTrue(threads)
        stopped.set()                                      # Nobody calls close().
        for thread in threads:
            thread.join(3)
        self.assertFalse(any(thread.is_alive() for thread in threads))

    def test_idle_read_ahead_threads_leave_after_a_forced_end(self):
        workers = Workers(4, threading.Event())
        gate = threading.Barrier(4)
        tasks = [workers.submit(gate.wait, 5) for _ in range(4)]
        for task in tasks:
            task.result(timeout=5)
        threads = list(workers.threads)
        self.assertEqual(len(threads), 4)
        for thread in threads:
            force_end(thread)                              # Neither cancelled nor closed.
        for thread in threads:
            thread.join(3)
        self.assertFalse(any(thread.is_alive() for thread in threads))

    def test_task_that_has_not_started_can_be_cancelled_and_a_started_one_cannot(self):
        stopped = threading.Event()
        self.addCleanup(stopped.set)
        workers = Workers(1, stopped)
        gate = threading.Event()
        running = workers.submit(gate.wait, 5)
        self.assertTrue(wait_until(lambda: running.state == core.Task.RUNNING))
        waiting = workers.submit(gate.wait, 5)
        self.assertFalse(running.cancel())
        self.assertTrue(waiting.cancel())
        self.assertTrue(waiting.done())
        gate.set()
        running.result(timeout=5)
        self.assertTrue(running.done())

    def test_discovery_threads_leave_after_a_forced_end(self):
        log = logging.getLogger('concurrent.futures')      # It reports each forced end as a crash.
        log.disabled = True
        self.addCleanup(setattr, log, 'disabled', False)
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        gate = threading.Barrier(4)
        for future in [executor.submit(gate.wait, 5) for _ in range(4)]:
            future.result(timeout=5)
        threads = list(executor._threads)
        self.assertEqual(len(threads), 4)
        for thread in threads:
            force_end(thread)
        end_executor(executor)                             # A plain shutdown strands three of the four.
        for thread in threads:
            thread.join(3)
        self.assertFalse(any(thread.is_alive() for thread in threads))

    def test_piece_store_threads_leave_when_the_session_is_cancelled_without_a_close(self):
        meta, payload = film(pieces=8)
        cancel = threading.Event()
        store = PieceStore(meta, max_cache_bytes=len(payload), allow_webseeds=False, cancel=cancel,
                           peer_fetcher=lambda index: payload[index * 16384:(index + 1) * 16384])
        self.addCleanup(store.close)
        store.configure_stream(0)
        self.assertEqual(b''.join(bytes(part) for part in store.read(0, 0, 40000)), payload[:40000])
        store.prefetch(range(8))
        self.assertTrue(wait_until(lambda: store.prefetch_executor is not None and store.prefetch_executor.threads))
        threads = list(store.prefetch_executor.threads)
        if store.readahead_thread is not None:
            threads.append(store.readahead_thread)
        self.assertTrue(any(thread.is_alive() for thread in threads))
        cancel.set()                                       # What a stop does first; nothing is closed.
        for thread in threads:
            thread.join(3)
        self.assertFalse(any(thread.is_alive() for thread in threads))

    def test_web_seed_that_answers_nonsense_is_a_failed_source_and_not_a_crash(self):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(2)
        self.addCleanup(listener.close)

        def nonsense():
            channel, _ = listener.accept()
            with channel:
                channel.recv(4096)
                channel.sendall(b'this is not http\r\n\r\n')
        threading.Thread(target=nonsense, daemon=True).start()
        with self.assertRaises(TorrentError) as raised:
            core._http_range('http://127.0.0.1:{}/film.mkv'.format(listener.getsockname()[1]), 0, 10, 10, 5)
        self.assertIn('Webseed protocol error', str(raised.exception))

    def test_cancelled_piece_does_not_go_on_to_the_web_seeds(self):
        meta, payload = film(extra={b'url-list': b'http://127.0.0.1:9/'})
        self.assertTrue(meta.webseeds)
        cancel = threading.Event()

        def swarm(index):
            cancel.set()                                   # The session was stopped while this piece was awaited.
            raise TorrentError('Cancelled')
        store = PieceStore(meta, peer_fetcher=swarm, cancel=cancel)
        self.addCleanup(store.close)
        asked = []
        with patch.object(core, 'fetch_webseed_piece', lambda *args, **kwargs: asked.append(args) or payload[:16384]):
            with self.assertRaises(TorrentError) as raised:
                store.piece(0)
        self.assertEqual(str(raised.exception), 'Cancelled')
        self.assertEqual(asked, [])
        self.assertEqual(dict(store.failures), {})         # A stop is not a failed source.


class DownloadsCloseTests(BackendCase):
    def test_journal_that_cannot_be_written_does_not_keep_the_downloads_running(self):
        downloads, calls = self.backend.downloads, []
        downloads.runtime['x'] = types.SimpleNamespace(interrupt=lambda: calls.append('interrupt'))
        self.addCleanup(downloads.runtime.clear)
        save = downloads._save

        def full_disk(*args, **kwargs):
            raise OSError(errno.ENOSPC, 'No space left on device')
        downloads._save = full_disk
        self.addCleanup(setattr, downloads, '_save', save)
        with self.assertRaises(OSError):
            downloads.close(0)
        self.assertEqual(calls, ['interrupt'])


class ClosedEngineTests(BackendCase):
    def test_no_session_starts_after_the_last_stop(self):
        with tempfile.TemporaryDirectory() as profile:
            engine = TorrentEngine(profile, lambda key: '', lambda *message: None)
            engine.stop(0, final=True)
            with patch('elementum_py.engine.resolve_magnet') as resolve:
                with self.assertRaises(TorrentError) as raised:
                    engine.prepare('magnet:?xt=urn:btih:' + 'a' * 40, peer_only=True)
                self.assertIn('closing', str(raised.exception))
                self.assertIn('cancel', str(raised.exception).lower())   # Never counted against the source.
                resolve.assert_not_called()
            self.assertIsNone(engine.preparing)
            meta, _ = film()
            folder = Path(engine.cache_root) / meta.info_hash.hex()
            folder.mkdir(parents=True)
            (folder / 'metadata.torrent').write_bytes(meta.raw)
            with self.assertRaises(TorrentError) as raised:
                engine.prepare_cached(meta.info_hash.hex(), 0)
            self.assertIn('closing', str(raised.exception))

    def test_ordinary_stop_does_not_close_the_engine(self):
        with tempfile.TemporaryDirectory() as profile:
            engine = TorrentEngine(profile, lambda key: '', lambda *message: None)
            engine.stop()
            self.assertFalse(engine.closed)
            self.assertIsNotNone(engine.begin())
            engine.stop()
            engine.interrupt()                             # A paused download: that engine is never used again.
            self.assertTrue(engine.closed)

    def test_engine_cannot_be_replaced_once_the_service_is_closing(self):
        self.backend.close(0)
        with self.assertRaises(TorrentError):
            self.backend.new_engine(self.folder.name, self.kodi.setting)
        self.assertIs(self.backend.engine, self.engine)

    def test_engine_swapped_in_just_before_the_close_is_the_one_that_is_stopped(self):
        first = self.backend.engine
        swapped = self.backend.new_engine(self.folder.name, self.kodi.setting)
        self.assertIsNot(swapped, first)
        self.backend.close(0)
        self.assertTrue(swapped.closed)

    def test_cancelled_preparation_frees_the_engine_at_once(self):
        meta, _ = film()
        silent = socket.socket()                           # Six dials that would time out for seven seconds.
        silent.bind(('127.0.0.1', 0))
        silent.listen(8)
        self.addCleanup(silent.close)
        with tempfile.TemporaryDirectory() as profile:
            engine = TorrentEngine(profile, lambda key: 'false' if key == 'dht_enabled' else '', lambda *message: None)
            outcome = {}

            def prepare():
                try:
                    engine.prepare('magnet:?xt=urn:btih:' + meta.info_hash.hex(), peer_only=True)
                except TorrentError as error:
                    outcome['error'] = str(error)
            with patch('elementum_py.engine.resolve_magnet', return_value=(meta, [silent.getsockname()])), \
                    patch('elementum_py.engine.tracker_peers', return_value=([], [])):
                thread = threading.Thread(target=prepare, daemon=True)
                thread.start()
                self.assertTrue(wait_until(lambda: engine.preparing_pool is not None and engine.preparing_pool.connecting))
                engine.stop()
                stopped = time.monotonic()
                thread.join(6)
                self.assertFalse(thread.is_alive())
                # It used to hold the engine and the play lock for up to ten more seconds.
                self.assertLess(time.monotonic() - stopped, 1.0)
                self.assertEqual(outcome.get('error'), 'Cancelled')
                self.assertTrue(engine.lock.acquire(blocking=False))
                engine.lock.release()


class DroppedStreamTests(unittest.TestCase):
    def setUp(self):
        self.length = 16384
        self.meta, self.payload = film(pieces=768, length=self.length)       # 12 MiB: more than any socket buffer.
        self.store = PieceStore(self.meta, max_cache_bytes=len(self.payload), allow_webseeds=False,
                                peer_fetcher=lambda index: self.payload[index * self.length:(index + 1) * self.length])
        self.addCleanup(self.store.close)
        self.closing = threading.Event()
        self.stream = StreamServer(self.store, 0, heartbeat=.05, closing=self.closing)
        self.addCleanup(self.stream.close, True)
        for index in range(len(self.meta.hashes)):
            self.store.piece(index)                        # Everything cached: the response never waits for a piece.

    def paused_player(self):
        connection = http.client.HTTPConnection('127.0.0.1', self.stream.port, timeout=10)
        self.addCleanup(connection.close)
        connection.connect()
        connection.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
        connection.request('GET', urlsplit(self.stream.url).path, headers={'Range': 'bytes=0-'})
        response = connection.getresponse()                # Headers only; the body is not read.
        self.addCleanup(response.close)                    # Kept open: a dropped response object would hang up.
        self.assertEqual(response.status, 206)
        self.assertTrue(wait_until(lambda: self.stream.stats()['open_responses'] == 1))
        delivered = -1
        while delivered != self.stream.stats()['delivered_bytes']:           # Until the socket buffers are full.
            delivered = self.stream.stats()['delivered_bytes']
            time.sleep(.3)
        self.assertLess(delivered, len(self.payload))
        return response

    def test_stopping_service_ends_a_response_that_a_paused_player_does_not_read(self):
        self.paused_player()
        self.stream.close(drop=True)
        self.assertTrue(wait_until(lambda: self.stream.stats()['open_responses'] == 0, 4))

    def test_stopping_service_also_ends_the_response_of_a_stream_that_was_closed_before(self):
        self.paused_player()
        self.stream.close()                                # The film was stopped; the player kept its connection.
        time.sleep(1.2)
        self.assertEqual(self.stream.stats()['open_responses'], 1)
        self.closing.set()                                 # Later the add-on is switched off or updated.
        self.assertTrue(wait_until(lambda: self.stream.stats()['open_responses'] == 0, 4))

    def test_ordinary_close_lets_the_player_finish_what_is_cached(self):
        response = self.paused_player()
        self.stream.close()                                # A switch to another source.
        time.sleep(1.5)
        self.assertEqual(self.stream.stats()['open_responses'], 1)
        self.assertEqual(response.read(), self.payload)    # Every byte, in order, across the timed writes.
        self.assertTrue(wait_until(lambda: self.stream.stats()['open_responses'] == 0, 4))


if __name__ == '__main__':
    unittest.main()
