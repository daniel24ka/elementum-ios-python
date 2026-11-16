"""Persistent, bounded on-device downloads with pause, resume and saved playback."""

import copy
import json
import os
import re
import secrets
import shutil
import threading
import time

from .core import TorrentError
from .engine import TorrentEngine


class Downloads:
    PUBLIC = ('id', 'title', 'status', 'created', 'updated', 'progress', 'completed_bytes', 'total_bytes',
              'file_index', 'files', 'selected_files', 'info_hash', 'error', 'rate', 'media')
    BUSY = ('preparing', 'downloading', 'verifying', 'pausing', 'cancelling')

    def __init__(self, profile, setting=lambda key: '', engine_factory=TorrentEngine, free_space=None):
        self.root = os.path.join(profile, 'python-downloads')
        os.makedirs(self.root, exist_ok=True)
        self.path = os.path.join(self.root, 'jobs.json')
        self.setting, self.engine_factory = setting, engine_factory
        self.free_space = free_space or (lambda path: shutil.disk_usage(path).free)
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self.jobs, self.runtime, self.workers, self.reservations = {}, {}, {}, {}
        self.memory_reservations = {}
        self.closed = threading.Event()
        self.load_error = ''
        try:
            with open(self.path, encoding='utf-8') as handle:
                saved = json.load(handle)
            if saved.get('version') != 1 or not isinstance(saved.get('jobs'), list):
                raise ValueError('Invalid download journal')
            for job in saved['jobs'][:100]:
                if not isinstance(job, dict) or not re.fullmatch(r'[a-f0-9]{24}', job.get('id', '')):
                    continue
                self._uri(job.get('uri', ''))
                if job.get('status') in self.BUSY + ('queued',):
                    job['status'] = 'paused'
                self.jobs[job['id']] = job
        except FileNotFoundError:
            pass
        except (OSError, ValueError, AttributeError, TorrentError):
            self.load_error = 'The saved download list could not be read'
        self.scheduler = threading.Thread(target=self._schedule, daemon=True)
        self.scheduler.start()

    @staticmethod
    def _uri(uri):
        if not isinstance(uri, str) or len(uri) > 65536 or not uri.startswith(('magnet:?', 'https://', 'http://')):
            raise TorrentError('Use a magnet link or an HTTP(S) torrent URL')
        if any(char in uri for char in '\r\n\0'):
            raise TorrentError('Invalid torrent URI')

    def _job(self, identity):
        if not isinstance(identity, str) or not re.fullmatch(r'[a-f0-9]{24}', identity) or identity not in self.jobs:
            raise TorrentError('Download not found')
        return self.jobs[identity]

    def folder(self, identity):
        self._job(identity)
        path = os.path.realpath(os.path.join(self.root, identity))
        root = os.path.realpath(self.root)
        if os.path.commonpath([root, path]) != root or path == root or os.path.islink(os.path.join(self.root, identity)):
            raise TorrentError('Unsafe download folder')
        return path

    def _save(self, force=False):
        # A delayed worker from a closing Kodi interpreter must never overwrite
        # the journal of the next service instance after restart.
        if self.closed.is_set() and not force:
            return
        if self.load_error and os.path.exists(self.path):
            os.replace(self.path, self.path + '.unreadable-' + secrets.token_hex(4))
            self.load_error = ''
        temporary = self.path + '.tmp'
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
            json.dump({'version': 1, 'jobs': list(self.jobs.values())}, handle)
        os.replace(temporary, self.path)

    def _set(self, job, **values):
        with self.changed:
            job.update(values, updated=time.time())
            self._save()
            self.changed.notify_all()

    def add(self, uri, title='', file_index=None, all_files=False, peer_only=False, media=None):
        self._uri(uri)
        if file_index is not None and (type(file_index) is not int or file_index < 0):
            raise TorrentError('Invalid selected file')
        with self.changed:
            if self.closed.is_set() or len(self.jobs) >= 100:
                raise TorrentError('The download queue is unavailable or full')
            for job in self.jobs.values():
                if job['uri'] == uri and job.get('requested_file_index', job.get('file_index')) == file_index and job.get('all_files', False) == bool(all_files):
                    return job['id']
            identity = secrets.token_hex(12)
            job = {'id': identity, 'uri': uri, 'title': str(title)[:500] or 'Torrent download',
                   'status': 'queued', 'created': time.time(), 'updated': time.time(), 'progress': 0,
                   'completed_bytes': 0, 'total_bytes': 0, 'file_index': file_index, 'requested_file_index': file_index,
                   'all_files': bool(all_files), 'peer_only': bool(peer_only), 'files': [], 'selected_files': [],
                   'info_hash': '', 'error': '', 'rate': 0,
                   'media': {key: str(value)[:40] for key, value in (media or {}).items()
                             if key in ('show', 'season', 'episode', 'tmdb', 'type')}}
            self.jobs[identity] = job
            self._save()
            self.changed.notify_all()
            return identity

    def snapshot(self, identity=None):
        with self.lock:
            jobs = [self._job(identity)] if identity else list(self.jobs.values())
            return [copy.deepcopy({key: job.get(key) for key in self.PUBLIC}) for job in jobs]

    def control(self, identity, action):
        if action not in ('pause', 'resume', 'cancel', 'recheck'):
            raise TorrentError('Unknown download action')
        with self.changed:
            job = self._job(identity)
            if self.closed.is_set():
                raise TorrentError('Downloads are closing')
            if action in ('resume', 'recheck'):
                if identity in self.workers:
                    raise TorrentError('Wait for this download to finish pausing')
                if action == 'recheck' and not job.get('info_hash'):
                    raise TorrentError('No saved torrent pieces to verify yet')
                if job['status'] != 'completed' or action == 'recheck':
                    self._set(job, status='queued', error='', recheck=action == 'recheck')
            elif job['status'] != 'completed':
                status = 'paused' if action == 'pause' else 'cancelled'
                engine = self.runtime.get(identity)
                self._set(job, status=('pausing' if action == 'pause' else 'cancelling') if engine else status,
                          requested_status=status)
                if engine:
                    engine.interrupt()
            self.changed.notify_all()
        return self.snapshot(identity)[0]

    def remove(self, identity, delete_data=False):
        with self.changed:
            job = self._job(identity)
            if identity in self.workers:
                raise TorrentError('Pause or cancel the download before removing it')
            folder = self.folder(identity)
            if delete_data and os.path.isdir(folder):
                # Both paths are resolved and verified by folder() before recursion.
                shutil.rmtree(folder)
            if not delete_data and os.path.exists(folder):
                raise TorrentError('Removing a saved download requires deleting its saved data')
            del self.jobs[identity]
            self._save()

    def playback(self, identity, file_index=None):
        with self.lock:
            job = self._job(identity)
            if job['status'] != 'completed':
                raise TorrentError('Wait for the download to finish before offline playback')
            selected = job['selected_files']
            file_index = job['file_index'] if file_index is None else file_index
            if file_index not in selected:
                raise TorrentError('This file was not downloaded')
            return {'profile': self.folder(identity), 'info_hash': job['info_hash'],
                    'file_index': file_index, 'title': job['title'], 'media': copy.deepcopy(job.get('media', {}))}

    def _parallelism(self):
        try:
            return max(1, min(3, int(self.setting('download_parallel') or '2')))
        except ValueError:
            return 2

    def _schedule(self):
        while not self.closed.is_set():
            with self.changed:
                for identity, job in self.jobs.items():
                    if len(self.workers) >= self._parallelism():
                        break
                    if job['status'] != 'queued' or identity in self.workers:
                        continue
                    self._set(job, status='preparing', requested_status='')
                    worker = threading.Thread(target=self._run, args=(identity,), daemon=True)
                    self.workers[identity] = worker
                    worker.start()
                self.changed.wait(0.5)

    def _run(self, identity):
        engine, final_status, failure = None, 'failed', ''
        try:
            with self.changed:
                job = self._job(identity)
                engine = self.engine_factory(self.folder(identity), self.setting)
                self.runtime[identity] = engine
                if self.closed.is_set() or job.get('requested_status'):
                    return
            if job.get('recheck'):
                prepared = engine.prepare_cached(job['info_hash'], job['file_index'], job['title'])
            else:
                prepared = engine.prepare(job['uri'], job.get('requested_file_index', job['file_index']), job['title'],
                                          peer_only=job.get('peer_only', False), streaming=False, force_disk=True)
            session = engine.active
            meta, store = session['meta'], session['store']
            # Large pieces temporarily coexist with cached pieces during transfer.
            # Reserve a shared download budget before fetching any piece bytes.
            budget = 96 * 1024 * 1024
            cost = min(budget, max(32 * 1024 * 1024, 2 * meta.piece_length))
            with self.changed:
                while sum(self.memory_reservations.values()) + cost > budget:
                    if self.closed.is_set() or job.get('requested_status'):
                        raise TorrentError('Download paused or cancelled')
                    self.changed.wait(0.2)
                self.memory_reservations[identity] = cost
            selected = list(range(len(meta.files))) if job.get('all_files') else [prepared['file_index']]
            pieces = set()
            for index in selected:
                item = meta.files[index]
                if item['length']:
                    pieces.update(range(item['offset'] // meta.piece_length,
                                        (item['offset'] + item['length'] - 1) // meta.piece_length + 1))
            pieces = sorted(pieces)
            total = sum(meta.piece_size(index) for index in pieces)
            existing, present = 0, set()
            for index in pieces:
                path = os.path.join(store.cache_dir, str(index) + '.piece')
                if os.path.isfile(path) and os.path.getsize(path) == meta.piece_size(index):
                    existing += meta.piece_size(index)
                    present.add(index)
            with self.changed:
                needed = max(0, total - existing)
                if not job.get('recheck') and needed + sum(self.reservations.values()) > self.free_space(self.root) - 512 * 1024 * 1024:
                    raise TorrentError('Not enough free storage for this download; free space or remove saved downloads')
                self.reservations[identity] = needed
                self._set(job, status='verifying' if job.get('recheck') else 'downloading', info_hash=meta.info_hash.hex(), file_index=prepared['file_index'],
                          files=[{'index': index, 'name': '/'.join(item['path']), 'bytes': item['length']}
                                 for index, item in enumerate(meta.files)], selected_files=selected, total_bytes=total)
            done, last_save, started = 0, time.monotonic(), time.monotonic()
            for position, index in enumerate(pieces):
                if self.closed.is_set() or job.get('requested_status'):
                    raise TorrentError('Download paused or cancelled')
                store.prefetch(pieces[position:position + store.prefetch_workers])
                store.piece(index)
                done += meta.piece_size(index)
                with self.changed:
                    job.update(completed_bytes=done, progress=round(100 * done / max(1, total), 2),
                               rate=round(done / max(0.01, time.monotonic() - started)))
                    if index not in present:
                        self.reservations[identity] = max(0, self.reservations[identity] - meta.piece_size(index))
                    if time.monotonic() - last_save >= 1:
                        self._save()
                        last_save = time.monotonic()
            final_status = 'completed'
        except Exception as error:
            # Network errors can include tracker passkeys: keep only useful safe text.
            failure = re.sub(r'https?://\S+', '[source]', str(error))[:250]
        finally:
            if engine:
                try:
                    engine.stop()
                except Exception:
                    failure = failure or 'Could not close download connections cleanly'
            with self.changed:
                job = self.jobs.get(identity)
                if job:
                    final_status = job.get('requested_status') or ('paused' if self.closed.is_set() else final_status)
                    self._set(job, status=final_status, rate=0, error='' if final_status != 'failed' else failure, recheck=False)
                self.reservations.pop(identity, None)
                self.memory_reservations.pop(identity, None)
                self.runtime.pop(identity, None)
                self.workers.pop(identity, None)
                self.changed.notify_all()

    def close(self):
        self.closed.set()
        with self.changed:
            engines, workers = list(self.runtime.values()), list(self.workers.values())
            for job in self.jobs.values():
                if job['status'] in self.BUSY + ('queued',):
                    job['status'] = 'paused'
                    job['requested_status'] = job.get('requested_status') or 'paused'
            self._save(force=True)
            self.changed.notify_all()
        for engine in engines:
            engine.interrupt()
        deadline = time.monotonic() + 10
        for worker in workers:
            worker.join(timeout=max(0, deadline - time.monotonic()))
        self.scheduler.join(timeout=1)
