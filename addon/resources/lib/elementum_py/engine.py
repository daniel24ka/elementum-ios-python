"""On-device torrent session manager; HTTP is only the local player interface."""

import json
import os
import re
import threading
import time
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from .core import Metainfo, PieceStore, TorrentError
from .discovery import PUBLIC_TRACKERS, dht_peers, resolve_magnet, tracker_peers
from .peers import PeerPool
from .streaming import StreamServer


class TorrentEngine:
    def __init__(self, profile, get_setting=lambda key: '', progress=None, choose_file=None):
        self.profile = profile
        self.get_setting = get_setting
        self.progress = progress or (lambda message: None)
        self.choose_file = choose_file
        self.lock = threading.RLock()
        self.active = None
        self.preparing = None
        self.cache_root = os.path.join(profile, 'python-torrents')
        os.makedirs(self.cache_root, exist_ok=True)

    def prepare(self, uri, file_index=None, title='', peer_only=False):
        with self.lock:
            self.stop()
            cancel = threading.Event()
            self.preparing = cancel
            pool = None
            stream = None
            store = None
            try:
                self.progress('Reading torrent metadata')
                peers = []
                if uri.startswith('magnet:?'):
                    meta, peers = resolve_magnet(uri, cancel=cancel, use_dht=self.get_setting('dht_enabled') != 'false')
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
                    with urlopen(request, timeout=15) as response:
                        metadata = response.read(2 * 1024 * 1024 + 1)
                    meta = Metainfo(metadata)
                    trackers = meta.trackers or ([] if meta.private else list(PUBLIC_TRACKERS))
                    self.progress('Finding torrent peers')
                    peers, _ = tracker_peers(trackers, meta.info_hash, meta.total_length, cancel=cancel)
                    if not meta.private and len(peers) < 12 and self.get_setting('dht_enabled') != 'false':
                        peers.extend(dht_peers(meta.info_hash, cancel=cancel))
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
                self.progress('Connecting to torrent peers')
                if peers:
                    pool = PeerPool(meta, peers, cancel)
                    try:
                        pool.prepare(timeout=15)
                    except TorrentError:
                        pool.close()
                        pool = None
                if pool is None and (not meta.webseeds or peer_only):
                    raise TorrentError('No usable torrent peers found')
                folder = os.path.join(self.cache_root, meta.info_hash.hex())
                os.makedirs(folder, exist_ok=True)
                with open(os.path.join(folder, 'metadata.torrent'), 'wb') as handle:
                    handle.write(meta.raw)
                use_disk = self.get_setting('download_storage') != '1'
                store = PieceStore(meta, peer_fetcher=pool.fetch if pool else None,
                                   cache_dir=os.path.join(folder, 'pieces') if use_disk else None,
                                   allow_webseeds=not peer_only, cancel=cancel)
                self.progress('Buffering verified torrent pieces')
                first = meta.files[file_index]['offset'] // meta.piece_length
                last = (meta.files[file_index]['offset'] + meta.files[file_index]['length'] - 1) // meta.piece_length
                store.prefetch(range(first, min(first + 5, last + 1)))
                store.piece(first)
                stream = StreamServer(store, file_index)
                session = {'meta': meta, 'pool': pool, 'store': store, 'stream': stream,
                           'cancel': cancel, 'file_index': file_index, 'uri': uri,
                           'title': title or meta.name, 'created': time.time()}
                if cancel.is_set():
                    raise TorrentError('Cancelled')
                self.active = session
                self.preparing = None
                return {'url': stream.url, 'info_hash': meta.info_hash.hex(), 'title': session['title'],
                        'selected_file': '/'.join(meta.files[file_index]['path']), 'file_index': file_index,
                        'source': 'TCP peers' if pool else 'HTTP webseed'}
            except Exception:
                cancel.set()
                if pool:
                    pool.close()
                if store:
                    store.close()
                if stream:
                    stream.close()
                self.preparing = None
                raise

    def stop(self):
        # Cancellation precedes taking the session lock to interrupt a pending prepare.
        if self.preparing is not None:
            self.preparing.set()
        session = self.active
        self.active = None
        if not session:
            return
        session['cancel'].set()
        if session['pool']:
            session['pool'].close()
        session['store'].close()
        session['stream'].close()

    def stats(self):
        session = self.active
        if not session:
            return {'active': False}
        result = {'active': True, 'title': session['title'], 'info_hash': session['meta'].info_hash.hex(),
                  'file_index': session['file_index'], 'verified_pieces': session['store'].verified_pieces,
                  'cached_pieces': len(session['store'].cache), 'last_source': session['store'].last_source,
                  'disk_cache_hits': session['store'].disk_hits}
        if session['pool']:
            result.update(session['pool'].stats())
        return result
