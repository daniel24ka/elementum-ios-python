"""On-device torrent session manager; HTTP is only the local player interface."""

import json
import os
import re
import threading
import time
from collections import OrderedDict
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from .core import Metainfo, PieceStore, TorrentError
from .discovery import PUBLIC_TRACKERS, dht_peers, parse_magnet, resolve_magnet, tracker_peers
from .peers import PeerPool
from .streaming import StreamServer


class TorrentEngine:
    def __init__(self, profile, get_setting=lambda key: '', progress=None, choose_file=None):
        self.profile = profile
        self.get_setting = get_setting
        self.progress = progress or (lambda message: None)
        self.choose_file = choose_file
        self.lock = threading.RLock()
        self.session_lock = threading.Lock()
        self.active = None
        self.preparing = None
        self.preparing_pool = None
        self.metadata_cache = OrderedDict()
        self.last_metadata_diagnostics = {}
        self.last_preparation = {}
        self.cache_root = os.path.join(profile, 'python-torrents')
        os.makedirs(self.cache_root, exist_ok=True)

    def prepare(self, uri, file_index=None, title='', peer_only=False, streaming=True, force_disk=False):
        with self.lock:
            self.stop()
            cancel = threading.Event()
            self.preparing = cancel
            pool = None
            stream = None
            store = None
            try:
                budget = max(30, min(180, int(self.get_setting('buffer_timeout') or '120') - 15))
            except ValueError:
                budget = 105
            deadline = time.monotonic() + budget
            buffering_done = threading.Event()
            def remaining():
                value = deadline - time.monotonic()
                if value <= 0:
                    raise TorrentError('This source did not buffer in time; try another source')
                return value
            self.last_preparation = {'status': 'preparing', 'stage': 'metadata', 'started': time.time()}
            try:
                self.progress('Reading torrent metadata')
                peers = []
                if uri.startswith('magnet:?'):
                    self.last_metadata_diagnostics = {}
                    parsed = parse_magnet(uri)
                    cache_key = (parsed['info_hash'], tuple(parsed['trackers']))
                    cached = self.metadata_cache.get(cache_key)
                    if cached and time.monotonic() - cached[2] < 300:
                        meta, peers = cached[:2]
                        self.metadata_cache.move_to_end(cache_key)
                        self.last_metadata_diagnostics = {'metadata_verified': True, 'cache_hit': True,
                                                          'candidate_peers': len(peers)}
                    else:
                        meta, peers = resolve_magnet(uri, timeout=min(45, remaining()), cancel=cancel, use_dht=self.get_setting('dht_enabled') != 'false',
                                                     diagnostics=self.last_metadata_diagnostics)
                        self.metadata_cache[cache_key] = (meta, peers, time.monotonic())
                        while len(self.metadata_cache) > 4:
                            self.metadata_cache.popitem(last=False)
                else:
                    if urlsplit(uri).scheme not in ('http', 'https'):
                        raise TorrentError('Use an HTTP(S) torrent URL or a magnet link')
                    parts = uri.split('|')
                    uri_url = parts[0]
                    headers = {'User-Agent': 'ElementumPython/0.2'}
                    allowed = {'user-agent', 'referer', 'cookie', 'authorization', 'accept', 'accept-language'}
                    for pair in parts[1:]:
                        key, separator, value = pair.partition('=')
                        if separator and key.lower() in allowed and '\r' not in value and '\n' not in value:
                            headers[key] = value
                    request = Request(uri_url, headers=headers)
                    with urlopen(request, timeout=min(15, remaining())) as response:
                        metadata = response.read(2 * 1024 * 1024 + 1)
                    meta = Metainfo(metadata)
                    trackers = meta.trackers or ([] if meta.private else list(PUBLIC_TRACKERS))
                    self.progress('Finding torrent peers')
                    peers, _ = tracker_peers(trackers, meta.info_hash, meta.total_length, timeout=min(10, remaining()), cancel=cancel)
                    if not meta.private and len(peers) < 12 and self.get_setting('dht_enabled') != 'false':
                        peers.extend(dht_peers(meta.info_hash, timeout=min(10, remaining()), cancel=cancel))
                if file_index is None:
                    videos = [(index, item) for index, item in enumerate(meta.files)
                              if item['path'][-1].lower().endswith(('.mp4', '.mkv', '.avi', '.webm', '.mov', '.m4v', '.ts'))]
                    if not videos:
                        raise TorrentError('No video files found in torrent')
                    episode = re.search(r'S(\d{1,2})E(\d{1,3})', title, re.I)
                    matched = []
                    if episode:
                        season_number, episode_number = map(int, episode.groups())
                        pattern = re.compile(r'(?:S0?{}[ ._-]*E0?{}|\b{}x0?{})(?!\d)'.format(
                            season_number, episode_number, season_number, episode_number), re.I)
                        matched = [pair for pair in videos if pattern.search('/'.join(pair[1]['path']))]
                    if matched:
                        file_index = max(matched, key=lambda pair: pair[1]['length'])[0]
                    elif len(videos) > 1 and self.choose_file:
                        selected = self.choose_file([' / '.join(item['path']) for _, item in videos])
                        if selected < 0:
                            raise TorrentError('File selection cancelled')
                        file_index = videos[selected][0]
                    else:
                        file_index = max(videos, key=lambda pair: pair[1]['length'])[0]
                if not 0 <= int(file_index) < len(meta.files) or meta.files[int(file_index)]['length'] <= 0:
                    raise TorrentError('Invalid selected torrent file')
                file_index = int(file_index)
                self.last_preparation.update(stage='peers', info_hash=meta.info_hash.hex(), file_index=file_index,
                                             piece_length=meta.piece_length, candidate_peers=len(peers))
                self.progress('Connecting to torrent peers')
                if peers:
                    priority = getattr(meta, 'metadata_peers', ())
                    self.last_preparation['responsive_peers_prioritized'] = len(priority)
                    pool = PeerPool(meta, peers, cancel, priority=priority)
                    with self.session_lock:
                        self.preparing_pool = pool
                    try:
                        pool.prepare(timeout=min(15, remaining()))
                    except TorrentError:
                        self.last_preparation['peer_stats'] = pool.stats()
                        pool.close()
                        with self.session_lock:
                            self.preparing_pool = None
                        pool = None
                if pool is None and (not meta.webseeds or peer_only):
                    raise TorrentError('No usable torrent peers found')
                if pool:
                    def rediscover():
                        found, _ = tracker_peers(meta.trackers, meta.info_hash, meta.total_length,
                                                 timeout=8, cancel=cancel)
                        if not meta.private and self.get_setting('dht_enabled') != 'false':
                            found.extend(dht_peers(meta.info_hash, timeout=8, cancel=cancel))
                        return found
                    pool.refresh(rediscover)
                folder = os.path.join(self.cache_root, meta.info_hash.hex())
                os.makedirs(folder, exist_ok=True)
                with open(os.path.join(folder, 'metadata.torrent'), 'wb') as handle:
                    handle.write(meta.raw)
                use_disk = force_disk or self.get_setting('download_storage') != '1'
                def fetch_piece(index):
                    return pool.fetch(index) if buffering_done.is_set() else pool.fetch(index, timeout=min(45, remaining()))
                try:
                    cache_mib = max(16,min(64,int(self.get_setting('stream_cache_mb') or '32')))
                except ValueError:
                    cache_mib = 32
                store = PieceStore(meta, max_cache_bytes=cache_mib*1024*1024, peer_fetcher=fetch_piece if pool else None,
                                   cache_dir=os.path.join(folder, 'pieces') if use_disk else None,
                                   allow_webseeds=not peer_only, cancel=cancel,
                                   prefetch_budget=(lambda: max(0, pool.stats()['unchoked_peers'] - 1)) if pool else None)
                self.progress('Buffering verified torrent pieces')
                self.last_preparation['stage'] = 'buffer'
                first = meta.files[file_index]['offset'] // meta.piece_length
                last = (meta.files[file_index]['offset'] + meta.files[file_index]['length'] - 1) // meta.piece_length
                # Kodi may read the tail immediately for MP4/MKV codec indexes.
                # Start and end pieces share the existing bounded prefetch budget.
                if streaming:
                    store.prefetch([first, last])
                    store.piece(first)
                    if last != first:
                        self.last_preparation['stage'] = 'video_index'
                        self.progress('Buffering the video seek index')
                        store.piece(last)
                if streaming and self.get_setting('adaptive_buffer') != 'false':
                    store.configure_stream(file_index)
                stream = StreamServer(store, file_index) if streaming else None
                session = {'meta': meta, 'pool': pool, 'store': store, 'stream': stream,
                           'cancel': cancel, 'file_index': file_index, 'uri': uri,
                           'title': title or meta.name, 'created': time.time()}
                with self.session_lock:
                    if cancel.is_set():
                        raise TorrentError('Cancelled')
                    self.active = session
                    self.preparing = None
                    self.preparing_pool = None
                    buffering_done.set()
                self.last_preparation.update(status='ready', stage='playing' if streaming else 'downloading',
                                             head_and_tail_verified=bool(streaming),
                                             elapsed_seconds=round(time.time() - self.last_preparation['started'], 2))
                return {'url': stream.url if stream else None, 'info_hash': meta.info_hash.hex(), 'title': session['title'],
                        'selected_file': '/'.join(meta.files[file_index]['path']), 'file_index': file_index,
                        'source': 'TCP peers' if pool else 'HTTP webseed'}
            except Exception as error:
                interrupted = cancel.is_set()
                self.last_preparation.update(status='failed', error_type=type(error).__name__,
                    error=re.sub(r'https?://\S+', '[source]', str(error))[:250],
                    elapsed_seconds=round(time.time() - self.last_preparation['started'], 2))
                if pool:
                    self.last_preparation['peer_stats'] = pool.stats()
                cancel.set()
                if pool:
                    pool.close()
                if store:
                    store.close()
                if stream:
                    stream.close()
                self.preparing = None
                self.preparing_pool = None
                if interrupted:
                    self.last_preparation.update(status='cancelled', error='Cancelled')
                    raise TorrentError('Cancelled') from None
                raise

    def stop(self):
        # Cancellation precedes taking the session lock to interrupt a pending prepare.
        with self.session_lock:
            if self.preparing is not None:
                self.preparing.set()
            session = self.active
            self.active = None
            pending_pool = self.preparing_pool
            self.preparing_pool = None
        if pending_pool:
            pending_pool.close()
        if not session:
            return
        session['cancel'].set()
        if session['pool']:
            session['pool'].close()
        session['store'].close()
        if session['stream']:
            session['stream'].close()

    def interrupt(self):
        """Signal cancellation and break reads without waiting for worker joins."""
        with self.session_lock:
            if self.preparing is not None:
                self.preparing.set()
            session = self.active
            pending_pool = self.preparing_pool
        if session:
            session['cancel'].set()
        for pool in ([session['pool']] if session and session['pool'] else []) + ([pending_pool] if pending_pool else []):
            if pool:
                with pool.lock:
                    peers = list(pool.sessions)
                for peer in peers:
                    peer.close()

    def prepare_cached(self, info_hash, file_index, title=''):
        """Play a selected saved file without discovery, trackers or webseeds."""
        if not re.fullmatch(r'[a-f0-9]{40}', info_hash):
            raise TorrentError('Invalid saved torrent identifier')
        with self.lock:
            self.stop()
            folder = os.path.join(self.cache_root, info_hash)
            with open(os.path.join(folder, 'metadata.torrent'), 'rb') as handle:
                meta = Metainfo(handle.read(2 * 1024 * 1024 + 1))
            if meta.info_hash.hex() != info_hash:
                raise TorrentError('Saved torrent metadata failed verification')
            if type(file_index) is not int or not 0 <= file_index < len(meta.files) or not meta.files[file_index]['length']:
                raise TorrentError('Invalid saved video file')
            cancel = threading.Event()
            self.preparing = cancel
            store, stream = None, None
            try:
                store = PieceStore(meta, cache_dir=os.path.join(folder, 'pieces'),
                                   allow_webseeds=False, cancel=cancel)
                first = meta.files[file_index]['offset'] // meta.piece_length
                store.piece(first)
                last = (meta.files[file_index]['offset'] + meta.files[file_index]['length'] - 1) // meta.piece_length
                store.piece(last)
                stream = StreamServer(store, file_index)
                session = {'meta': meta, 'pool': None, 'store': store, 'stream': stream,
                           'cancel': cancel, 'file_index': file_index, 'uri': '',
                           'title': title or meta.name, 'created': time.time(), 'offline': True}
                with self.session_lock:
                    if cancel.is_set():
                        raise TorrentError('Cancelled')
                    self.active, self.preparing = session, None
                return {'url': stream.url, 'info_hash': info_hash, 'title': session['title'],
                        'file_index': file_index, 'selected_file': '/'.join(meta.files[file_index]['path']),
                        'source': 'Verified saved pieces'}
            except Exception:
                cancel.set()
                self.preparing = None
                if stream:
                    stream.close()
                if store:
                    store.close()
                raise

    def stats(self):
        session = self.active
        if not session:
            return {'active': False}
        with session['store'].lock:
            cached_bytes = sum(len(value) for value in session['store'].cache.values())
        result = {'active': True, 'title': session['title'], 'info_hash': session['meta'].info_hash.hex(),
                  'offline': bool(session.get('offline')), 'download_id': session.get('download'),
                  'file_index': session['file_index'], 'verified_pieces': session['store'].verified_pieces,
                  'cached_pieces': len(session['store'].cache), 'last_source': session['store'].last_source,
                  'disk_cache_hits': session['store'].disk_hits}
        result.update({'piece_length': session['meta'].piece_length, 'cached_bytes': cached_bytes,
                       'prefetch_workers': session['store'].prefetch_workers})
        result['buffer'] = session['store'].buffer_stats()
        if session['stream']:
            result['http'] = session['stream'].stats()
        if session['pool']:
            result.update(session['pool'].stats())
        return result
