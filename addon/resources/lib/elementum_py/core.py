"""Validated v1 metainfo, piece integrity and a bounded verified-piece cache."""

import hashlib
import concurrent.futures
import os
import socket
import struct
import threading
import time
from collections import OrderedDict, deque
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen


class TorrentError(Exception):
    pass


class Abandoned(TorrentError):
    """A read-ahead piece that nobody waits for was dropped after a seek."""


def bencode(value):
    if isinstance(value, bytes):
        return str(len(value)).encode('ascii') + b':' + value
    if isinstance(value, int):
        return b'i' + str(value).encode('ascii') + b'e'
    if isinstance(value, list):
        return b'l' + b''.join(bencode(item) for item in value) + b'e'
    if isinstance(value, dict) and all(isinstance(key, bytes) for key in value):
        return b'd' + b''.join(bencode(key) + bencode(value[key])
                              for key in sorted(value)) + b'e'
    raise TorrentError('Unsupported bencode type')


def bdecode(data, prefix=False, strict=True):
    """Keep metainfo canonical; tolerate unordered peer-control dictionaries.

    Duplicate keys and malformed integers remain invalid in both modes. Relaxed
    decoding is never used for the info dictionary whose raw bytes are hashed.
    """
    if not isinstance(data, bytes) or len(data) > 2 * 1024 * 1024:
        raise TorrentError('Metainfo must be bytes, at most 2 MiB')
    position = 0
    nodes = 0

    def parse(depth=0):
        nonlocal position, nodes
        nodes += 1
        if depth > 32 or nodes > 100000 or position >= len(data):
            raise TorrentError('Invalid or excessively nested bencode')
        tag = data[position:position + 1]
        if tag == b'i':
            end = data.find(b'e', position + 1)
            if end < 0:
                raise TorrentError('Unterminated integer')
            token = data[position + 1:end]
            if (not token or len(token) > 20 or token == b'-0' or
                    (token[:1] == b'-' and (len(token) < 2 or token[1:2] == b'0')) or
                    (len(token) > 1 and token[:1] == b'0') or
                    not token.lstrip(b'-').isdigit() or token.count(b'-') > 1):
                raise TorrentError('Noncanonical integer')
            position = end + 1
            return int(token)
        if tag in (b'l', b'd'):
            position += 1
            result = [] if tag == b'l' else {}
            previous = None
            while position < len(data) and data[position:position + 1] != b'e':
                key = parse(depth + 1)
                if tag == b'l':
                    result.append(key)
                else:
                    if (not isinstance(key, bytes) or key in result or
                            (strict and previous is not None and key <= previous)):
                        raise TorrentError('Dictionary keys must be unique and sorted')
                    result[key] = parse(depth + 1)
                    previous = key
            if position >= len(data):
                raise TorrentError('Unterminated container')
            position += 1
            return result
        if tag.isdigit():
            colon = data.find(b':', position)
            if colon < 0 or colon - position > 9:
                raise TorrentError('Invalid string length')
            token = data[position:colon]
            if not token.isdigit() or (len(token) > 1 and token[:1] == b'0'):
                raise TorrentError('Noncanonical string length')
            length = int(token)
            position = colon + 1
            end = position + length
            if end > len(data):
                raise TorrentError('Truncated string')
            value = data[position:end]
            position = end
            return value
        raise TorrentError('Invalid bencode token')

    result = parse()
    if prefix:
        return result, position
    if position != len(data):
        raise TorrentError('Trailing bencode data')
    return result


def _component(value):
    if not isinstance(value, bytes):
        raise TorrentError('Invalid path component')
    try:
        text = value.decode('utf-8')
    except UnicodeDecodeError as error:
        raise TorrentError('Path is not UTF-8') from error
    if (not text or text in ('.', '..') or any(char in text for char in '/\\\0') or
            ':' in text or any(ord(char) < 32 for char in text)):
        raise TorrentError('Unsafe path component')
    return text


class Metainfo:
    def __init__(self, data):
        root = bdecode(data)
        if not isinstance(root, dict) or not isinstance(root.get(b'info'), dict):
            raise TorrentError('No info dictionary')
        info = root[b'info']
        self.raw = data
        self.info_bytes = bencode(info)
        self.private = info.get(b'private') == 1
        self.trackers = []
        tiers = root.get(b'announce-list', [])
        if not isinstance(tiers, list):
            raise TorrentError('Invalid tracker tiers')
        candidates = [root.get(b'announce')] + [url for tier in tiers if isinstance(tier, list) for url in tier]
        for value in candidates[:64]:
            if isinstance(value, bytes):
                try:
                    url = value.decode('utf-8')
                except UnicodeDecodeError:
                    continue
                if urlsplit(url).scheme in ('http', 'https', 'udp') and url not in self.trackers:
                    self.trackers.append(url)
        if b'meta version' in info:
            raise TorrentError('Only v1 torrents are supported by this experiment')
        self.name = _component(info.get(b'name'))
        self.info_hash = hashlib.sha1(bencode(info)).digest()
        self.piece_length = info.get(b'piece length')
        if (type(self.piece_length) is not int or
                not 16384 <= self.piece_length <= 64 * 1024 * 1024):
            raise TorrentError('Piece length must be 16 KiB to 64 MiB')
        hashes = info.get(b'pieces')
        if not isinstance(hashes, bytes) or len(hashes) % 20:
            raise TorrentError('Invalid piece hashes')
        self.hashes = [hashes[start:start + 20] for start in range(0, len(hashes), 20)]
        self.multifile = b'files' in info
        if self.multifile == (b'length' in info):
            raise TorrentError('Expected either length or files')
        entries = info[b'files'] if self.multifile else [{b'length': info[b'length']}]
        if not isinstance(entries, list) or not entries or len(entries) > 10000:
            raise TorrentError('Invalid files list')
        self.files = []
        self.total_length = 0
        names = set()
        for entry in entries:
            if not isinstance(entry, dict):
                raise TorrentError('Invalid file entry')
            length = entry.get(b'length')
            if type(length) is not int or not 0 <= length <= 1024 ** 4:
                raise TorrentError('Invalid file length')
            parts = entry.get(b'path') if self.multifile else [info[b'name']]
            if not isinstance(parts, list) or not parts or len(parts) > 32:
                raise TorrentError('Invalid file path')
            path = tuple(_component(part) for part in parts)
            if path in names:
                raise TorrentError('Duplicate file path')
            names.add(path)
            self.files.append({'path': path, 'length': length, 'offset': self.total_length})
            self.total_length += length
        if not self.total_length or len(self.hashes) != (
                self.total_length + self.piece_length - 1) // self.piece_length:
            raise TorrentError('Piece count does not match content length')
        urls = root.get(b'url-list', [])
        if isinstance(urls, bytes):
            urls = [urls]
        if not isinstance(urls, list) or len(urls) > 32:
            raise TorrentError('Invalid webseed list')
        self.webseeds = []
        for value in urls:
            if isinstance(value, bytes):
                url = value.decode('utf-8')
                if urlsplit(url).scheme in ('http', 'https'):
                    self.webseeds.append(url)

    def piece_size(self, index):
        if not 0 <= index < len(self.hashes):
            raise TorrentError('Piece index out of bounds')
        return min(self.piece_length, self.total_length - index * self.piece_length)

    def verify(self, index, content):
        if len(content) != self.piece_size(index) or hashlib.sha1(content).digest() != self.hashes[index]:
            raise TorrentError('Piece SHA-1 verification failed')
        return content


def _receive(sock, count, deadline):
    chunks = bytearray()
    while len(chunks) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TorrentError('Peer deadline exceeded')
        sock.settimeout(min(remaining, 10))
        chunk = sock.recv(count - len(chunks))
        if not chunk:
            raise TorrentError('Peer disconnected')
        chunks.extend(chunk)
    return bytes(chunks)


def fetch_peer_piece(meta, address, index, timeout=20):
    """Fetch and verify one piece from an explicit TCP seed; no peer discovery."""
    size = meta.piece_size(index)
    deadline = time.monotonic() + timeout
    handshake = b'\x13BitTorrent protocol' + bytes(8) + meta.info_hash + b'-PT0001-' + os.urandom(12)
    with socket.create_connection(address, timeout=min(timeout, 10)) as peer:
        peer.sendall(handshake)
        response = _receive(peer, 68, deadline)
        if response[:20] != handshake[:20] or response[28:48] != meta.info_hash:
            raise TorrentError('Peer handshake mismatch')
        peer.sendall(struct.pack('!IB', 1, 2))  # interested
        wanted = [(start, min(16384, size - start)) for start in range(0, size, 16384)]
        pending = {}
        blocks = {}
        choked = True
        while len(blocks) < len(wanted):
            if not choked:
                for start, length in wanted:
                    if start not in blocks and start not in pending:
                        peer.sendall(struct.pack('!IBIII', 13, 6, index, start, length))
                        pending[start] = length
                        if len(pending) >= 8:
                            break
            length = struct.unpack('!I', _receive(peer, 4, deadline))[0]
            if not length:
                continue
            if length > 131072:
                raise TorrentError('Peer message too large')
            message = _receive(peer, length, deadline)
            if message[0] == 0:
                choked = True
                pending.clear()
            elif message[0] == 1:
                choked = False
            elif message[0] == 7:
                if len(message) < 9:
                    raise TorrentError('Truncated piece response')
                part_index, start = struct.unpack('!II', message[1:9])
                if part_index != index or start not in pending or len(message) - 9 != pending[start]:
                    raise TorrentError('Unrequested or invalid piece block')
                blocks[start] = message[9:]
                del pending[start]
        return meta.verify(index, b''.join(blocks[start] for start, _ in wanted))


def _http_range(url, start, length, total, timeout):
    if urlsplit(url).scheme not in ('http', 'https'):
        raise TorrentError('Only HTTP(S) webseeds are supported')
    request = Request(url, headers={'Range': 'bytes={}-{}'.format(start, start + length - 1),
                                   'Accept-Encoding': 'identity',
                                   'User-Agent': 'PortableTorrentLab/0.1'})
    with urlopen(request, timeout=timeout) as response:
        if urlsplit(response.geturl()).scheme not in ('http', 'https'):
            raise TorrentError('Invalid webseed redirect')
        if response.status == 206:
            expected = 'bytes {}-{}/{}'.format(start, start + length - 1, total)
            if response.headers.get('Content-Range') != expected:
                raise TorrentError('Webseed returned incorrect Content-Range')
        elif response.status != 200 or start != 0 or length != total:
            raise TorrentError('Webseed did not honor the requested range')
        data = response.read(length + 1)
        if len(data) != length:
            raise TorrentError('Incorrect webseed response size')
        return data


def fetch_webseed_piece(meta, base, index, timeout=15):
    start = index * meta.piece_length
    end = start + meta.piece_size(index)
    content = bytearray()
    for item in meta.files:
        lo = max(start, item['offset'])
        hi = min(end, item['offset'] + item['length'])
        if lo >= hi:
            continue
        if meta.multifile:
            if not base.endswith('/'):
                raise TorrentError('Multi-file webseed must end with a slash')
            parts = (meta.name,) + item['path']
            url = base + '/'.join(quote(part, safe='') for part in parts)
        else:
            url = base + quote(meta.name, safe='') if base.endswith('/') else base
        content.extend(_http_range(url, lo - item['offset'], hi - lo, item['length'], timeout))
    return meta.verify(index, bytes(content))


class PieceStore:
    """Bound memory and share verified pieces across HTTP range requests.

    ``scheduler`` is optional. It lets a block-level swarm know which piece a
    reader is blocked on (``fetch(index, urgent)``, ``promote(index)``) and
    which read-ahead piece became pointless after a seek (``abandon(index)``).
    """
    RATE_WINDOW = 8.0
    PARKED_AFTER = 2.0   # A reader silent for this long is a connection Kodi parked.

    def __init__(self, meta, peers=(), max_cache_bytes=16 * 1024 * 1024,
                 peer_fetcher=None, cache_dir=None, allow_webseeds=True, cancel=None, prefetch_budget=None,
                 scheduler=None):
        self.meta = meta
        self.peers = tuple(peers)
        self.limit = max(1, max_cache_bytes // meta.piece_length)
        self.cache = OrderedDict()
        self.lock = threading.Lock()
        self.peer_fetcher = peer_fetcher
        self.scheduler = scheduler
        self.prefetch_budget = prefetch_budget
        self.cache_dir = cache_dir
        self.allow_webseeds = allow_webseeds
        self.cancel = cancel or threading.Event()
        self.inflight = {}             # index -> [ready event, still only read-ahead, waiting readers]
        self.failures = OrderedDict()  # index -> why its last download failed
        self.verified_pieces = 0
        self.last_source = None
        self.disk_hits = 0
        self.prefetch_executor = None
        self.prefetch_futures = {}
        self.prefetch_workers = max(1, min(3, max_cache_bytes // meta.piece_length))
        if scheduler is not None:
            # Blocks of several pieces keep every peer's pipeline full; the
            # in-flight buffers stay near 4 MiB whatever the piece length is.
            self.prefetch_workers = max(1, min(8, 4 * 1024 * 1024 // meta.piece_length, max(1, self.limit // 4)))
        self.network_bytes = 0
        self.network_seconds = 0
        self.samples = deque()         # (time, verified network bytes)
        self.first_sample = 0
        self.window = ()
        self.window_generation = 0
        self.read_sequence = 0
        self.anonymous = 0             # Readers outside the stream window get ids below zero.
        self.readers = {}
        self.reader_seen = {}
        self.challenger = (0, 0.0)     # An older reader that started moving while the owner is silent.
        self.read_cursor = None
        self.stream_file = None
        self.pinned = frozenset()
        self.start_bytes = 0           # Read-ahead wanted before the first reader connects.
        self.bitrate = 0
        self.caching = False
        self.evictions = 0
        self.readahead_event = threading.Event()
        self.readahead_thread = None
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)

    def piece(self, index, background=False, reader=0, gone=None):
        """A verified piece.

        ``reader`` identifies the reader waiting for it: a stream reader's
        generation, or a negative number for any other reader. ``gone`` is set
        when that reader has closed, so a download started on its behalf is not
        started again.
        """
        self.meta.piece_size(index)
        if self.cancel.is_set():
            raise TorrentError('Cancelled')
        promote = False
        with self.lock:
            if index in self.cache:
                self.cache.move_to_end(index)
                return self.cache[index]
            # A stream reader that does not own the window (left behind by a seek)
            # still gets its piece, without urgency.
            urgent = not background and not (reader > 0 and reader != self.window_generation)
            if index in self.inflight:
                flight = self.inflight[index]
                owner = False
                if flight[1] and urgent:
                    flight[1] = False      # A reader now depends on this read-ahead piece.
                    promote = True
            else:
                flight = [threading.Event(), not urgent, set()]
                self.inflight[index] = flight
                owner = True
            if not background:
                flight[2].add(reader)
        ready = flight[0]
        if not owner:
            if promote and self.scheduler is not None:
                self.scheduler.promote(index)
            while not ready.wait(0.1):
                if self.cancel.is_set():
                    raise TorrentError('Cancelled')
                if gone is not None and gone.is_set():
                    raise Abandoned('The reader of this piece has closed')
            with self.lock:
                if index in self.cache:
                    self.cache.move_to_end(index)
                    return self.cache[index]
            if gone is not None and gone.is_set():
                raise Abandoned('The reader of this piece has closed')
            return self.piece(index, background, reader, gone)
        try:
            started = time.monotonic()
            errors = []
            content = None
            from_disk = False
            if self.cache_dir:
                filename = os.path.join(self.cache_dir, str(index) + '.piece')
                try:
                    with open(filename, 'rb') as handle:
                        content = self.meta.verify(index, handle.read(self.meta.piece_size(index) + 1))
                    self.disk_hits += 1
                    from_disk = True
                    self.last_source = 'Verified disk cache'
                except (OSError, TorrentError) as error:
                    content = None
            if content is None and (self.scheduler is not None or self.peer_fetcher is not None):
                try:
                    if self.scheduler is not None:
                        fetched = self.scheduler.fetch(index, not flight[1])
                    else:
                        fetched = self.peer_fetcher(index)
                    content = self.meta.verify(index, fetched)
                    self.last_source = 'TCP peer'
                except Abandoned:
                    raise
                except (OSError, TorrentError) as error:
                    errors.append(str(error))
            if content is None:
                for address in self.peers:
                    try:
                        content = fetch_peer_piece(self.meta, address, index)
                        self.last_source = 'TCP peer'
                        break
                    except (OSError, TorrentError) as error:
                        errors.append(str(error))
            if content is None and self.allow_webseeds:
                for url in self.meta.webseeds:
                    try:
                        content = fetch_webseed_piece(self.meta, url, index)
                        self.last_source = 'HTTP webseed'
                        break
                    except (OSError, TorrentError) as error:
                        errors.append(str(error))
            if content is None:
                with self.lock:
                    self.failures[index] = '; '.join(errors[-2:])[:200]
                    while len(self.failures) > 16:
                        self.failures.popitem(last=False)
                raise TorrentError('No usable piece source: ' + '; '.join(errors[-2:]))
            if self.cancel.is_set():
                raise TorrentError('Cancelled')
            if self.cache_dir and not from_disk:
                filename = os.path.join(self.cache_dir, str(index) + '.piece')
                temporary = filename + '.tmp'
                with open(temporary, 'wb') as handle:
                    handle.write(content)
                os.replace(temporary, filename)
            with self.lock:
                if not from_disk:
                    now = time.monotonic()
                    self.network_bytes += len(content)
                    self.network_seconds += max(.001, now - started)
                    self.first_sample = self.first_sample or started
                    self.samples.append((now, len(content)))
                self.cache[index] = content
                self._trim(index)
                self.verified_pieces += 1
                self.failures.pop(index, None)
            return content
        finally:
            with self.lock:
                self.inflight.pop(index, None)
                ready.set()

    def settle(self, index, timeout=1.0):
        """Wait briefly until no download of this piece is running."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                flight = self.inflight.get(index)
            if flight is None or flight[0].wait(.05):
                return

    def _trim(self, keep):
        """Evict within the limit, never a piece the reader is about to need.

        Plain LRU order discards the oldest *inserted* piece, which during
        playback is the read-ahead piece right in front of the reader.
        """
        if len(self.cache) <= self.limit:
            return
        protected = set(self.window)
        protected.update(self.pinned)
        protected.add(keep)
        for position in self.readers.values():
            protected.add(position // self.meta.piece_length)
        for index in list(self.cache):
            if len(self.cache) <= self.limit:
                return
            if index not in protected:
                del self.cache[index]
                self.evictions += 1
        # Only protected pieces remain (many parallel readers, or a tiny cache):
        # give up the one that is furthest from the current reader.
        cursor = (self.read_cursor or 0) // self.meta.piece_length
        while len(self.cache) > self.limit:
            candidates = [index for index in self.cache if index != keep]
            if not candidates:
                return
            victim = max(candidates, key=lambda index: (index not in self.pinned, abs(index - cursor)))
            del self.cache[victim]
            self.evictions += 1

    def rate(self):
        """Verified network bytes per second over a short wall-clock window.

        Parallel piece downloads are counted once, unlike a sum of per-piece
        durations, so the result is the real throughput of the whole swarm.
        """
        now = time.monotonic()
        with self.lock:
            return self._rate(now)

    def _rate(self, now):
        horizon = now - self.RATE_WINDOW
        while self.samples and self.samples[0][0] < horizon:
            self.samples.popleft()
        if not self.samples:
            return 0
        span = max(1.0, now - max(horizon, self.first_sample))
        return sum(size for _, size in self.samples) / span

    def prefetch(self, indexes):
        capacity = min(self.prefetch_workers, max(0, int(self.prefetch_budget()))) if self.prefetch_budget else self.prefetch_workers
        if not capacity:
            return
        with self.lock:
            if self.cancel.is_set():
                return
            if self.prefetch_executor is None:
                self.prefetch_executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.prefetch_workers)
            self.prefetch_futures = {index: future for index, future in self.prefetch_futures.items() if not future.done()}
            for index in indexes:
                if (0 <= index < len(self.meta.hashes) and index not in self.cache and
                        index not in self.prefetch_futures and len(self.prefetch_futures) < capacity):
                    self.prefetch_futures[index] = self.prefetch_executor.submit(self._prefetch, index)

    def _prefetch(self, index):
        # A completed Future must not retain another large piece outside the LRU.
        # Foreground reads retry failed prefetches through the normal source path.
        try:
            self.piece(index, background=True)
        except (TorrentError, OSError):
            pass
        finally:
            self.readahead_event.set()

    def configure_stream(self, file_index):
        self.stream_file = file_index
        item = self.meta.files[file_index]
        if self.limit >= 16 and item['length']:
            # The container header and index are read again on every reopen and
            # give the subtitle hash; two pieces are cheap in a cache this large.
            self.pinned = frozenset((item['offset'] // self.meta.piece_length,
                                     (item['offset'] + item['length'] - 1) // self.meta.piece_length))

    def prime(self, start_bytes=0):
        """Start filling the window at the file start before the player connects."""
        if self.stream_file is None:
            return
        with self.lock:
            if self.read_cursor is not None or self.readers or self.cancel.is_set():
                return
            self.start_bytes = start_bytes
        self._window_at(self.meta.files[self.stream_file]['offset'], self.window_generation)

    def update_playback(self, duration, caching=False):
        if self.stream_file is None:
            return
        with self.lock:
            if duration > 0:
                self.bitrate = self.meta.files[self.stream_file]['length'] / duration
            self.caching = caching
        self.readahead_event.set()

    def _window_at(self, position, generation):
        from .playback_policy import read_ahead_pieces
        abandoned = []
        with self.lock:
            if generation in self.readers:
                now = time.monotonic()
                self.readers[generation] = position
                self.reader_seen[generation] = now
                owner = self.window_generation
                if owner not in self.readers:
                    self.window_generation = generation   # The previous owner closed; a live reader takes over.
                elif owner == generation:
                    self.challenger = (0, 0.0)
                elif now - self.reader_seen.get(owner, 0) > self.PARKED_AFTER:
                    # The newest connection is silent while this older one is read:
                    # Kodi went back to it. One burst proves nothing, because a parked
                    # connection still drains the piece it was waiting for, so the
                    # window moves only when this reader keeps advancing.
                    if self.challenger[0] != generation:
                        self.challenger = (generation, now)
                    elif now - self.challenger[1] >= self.PARKED_AFTER:
                        self.window_generation = generation
                        self.challenger = (0, 0.0)
            if generation != self.window_generation or self.cancel.is_set():
                return
            self.read_cursor = position
            index = position // self.meta.piece_length
            item = self.meta.files[self.stream_file]
            final = (item['offset']+item['length']-1)//self.meta.piece_length
            count = read_ahead_pieces(self.meta.piece_length, self.limit - len(self.pinned), self.bitrate,
                                      self._rate(time.monotonic()), self.caching)
            if self.start_bytes and not self.readers:
                # The start buffer is counted from the file's first byte, which
                # may lie inside its first piece: cover it completely.
                needed = -(-(position % self.meta.piece_length + self.start_bytes) // self.meta.piece_length)
                count = min(max(count, needed), max(0, self.limit - len(self.pinned) - 2))
            self.window = tuple(range(index,min(final+1,index+count)))
            # Cancel only work that has not started. In-flight verified reads
            # remain shared and valid; arbitrary parallel HTTP ranges still work.
            for old, future in list(self.prefetch_futures.items()):
                if old not in self.window and future.cancel():
                    self.prefetch_futures.pop(old,None)
            if self.scheduler is not None:
                # A started read-ahead piece outside the new window has no reader
                # waiting for it: release its peers for the new position.
                for old in self.prefetch_futures:
                    flight = self.inflight.get(old)
                    if (old not in self.window and old not in self.pinned and flight is not None
                            and flight[1] and not flight[2]):
                        abandoned.append(old)
            if self.readahead_thread is None:
                self.readahead_thread = threading.Thread(target=self._read_ahead,daemon=True)
                self.readahead_thread.start()
        if self.scheduler is not None:
            self.scheduler.focus(index)
        for old in abandoned:
            self.scheduler.abandon(old)
        self.readahead_event.set()

    def _read_ahead(self):
        while not self.cancel.is_set():
            self.readahead_event.wait(.5)
            self.readahead_event.clear()
            with self.lock:
                window = self.window
            self.prefetch(window)

    def buffer_stats(self):
        with self.lock:
            end = self.read_cursor
            if end is not None and self.stream_file is not None:
                item = self.meta.files[self.stream_file]
                maximum = item['offset']+item['length']
                while end < maximum:
                    index,inside = divmod(end,self.meta.piece_length)
                    content = self.cache.get(index)
                    if content is None:
                        break
                    end += min(len(content)-inside,maximum-end)
            ahead = max(0,end-self.read_cursor) if end is not None else 0
            rate = self._rate(time.monotonic())
            return {'cache_limit_bytes':self.limit*self.meta.piece_length,
                    'window_verified':all(index in self.cache for index in self.window),
                    'ahead_verified_bytes':ahead,'ahead_seconds_estimate':round(ahead/self.bitrate,2) if self.bitrate else None,
                    'target_ahead_pieces':len(self.window),'read_generation':self.read_sequence,
                    'network_verified_bytes':self.network_bytes,
                    'verified_rate':round(rate), 'bitrate_estimate':round(self.bitrate),
                    'pinned_pieces':len(self.pinned), 'inflight_pieces':len(self.inflight),
                    'evicted_pieces':self.evictions, 'open_readers':len(self.readers)}

    def close(self):
        self.cancel.set()
        self.readahead_event.set()
        if self.readahead_thread and self.readahead_thread is not threading.current_thread():
            self.readahead_thread.join(timeout=2)
        if self.prefetch_executor is not None:
            for future in self.prefetch_futures.values():
                future.cancel()
            self.prefetch_executor.shutdown(wait=True)

    def _left_behind(self, generation):
        """A seek started reader ``generation``: stop treating older readers as urgent.

        Kodi parks the previous connection instead of closing it. A piece that
        only such a reader waits for keeps downloading, so the connection stays
        valid if Kodi returns to it, but it no longer competes with the seek.
        """
        demoted = []
        with self.lock:
            for index, flight in self.inflight.items():
                waiting = flight[2]
                if not flight[1] and waiting and all(reader > 0 and reader != generation for reader in waiting):
                    flight[1] = True
                    demoted.append(index)
        for index in demoted:
            self.scheduler.demote(index)

    def _given_up(self, index, reader):
        """The connection closed while its piece was downloading: nobody reads it."""
        with self.lock:
            flight = self.inflight.get(index)
            if flight is None:
                return
            flight[2].discard(reader)
            if flight[2] or index in self.window or index in self.pinned or self.scheduler is None:
                return
            flight[1] = True
        self.scheduler.demote(index)
        self.scheduler.abandon(index)

    def _still_wanted(self, index, generation):
        """The reader that owns the window is waiting: its piece is urgent again."""
        with self.lock:
            flight = self.inflight.get(index)
            if flight is None or not flight[1] or generation != self.window_generation:
                return
            flight[1] = False
        if self.scheduler is not None:
            self.scheduler.promote(index)

    def _awaited(self, index, heartbeat, generation=None, reader=0):
        """Return a piece, yielding an empty chunk each heartbeat while it downloads.

        The caller can then keep its HTTP response alive during a stall. The
        download runs on its own thread, so closing the reader stays immediate.
        """
        with self.lock:
            content = self.cache.get(index)
            if content is not None:
                self.cache.move_to_end(index)
                return content
        outcome = []
        done = threading.Event()
        gone = threading.Event()

        def work():
            try:
                outcome.append(self.piece(index, reader=reader, gone=gone))
            except Exception as error:
                outcome.append(error)
            finally:
                done.set()

        threading.Thread(target=work, daemon=True).start()
        try:
            while not done.wait(heartbeat):
                if generation is not None:
                    self._still_wanted(index, generation)
                yield b''
        except GeneratorExit:
            gone.set()
            self._given_up(index, reader)
            raise
        if isinstance(outcome[0], Exception):
            raise outcome[0]
        return outcome[0]

    def read(self, file_index, offset, length, heartbeat=None):
        item = self.meta.files[file_index]
        if offset < 0 or length < 0 or offset + length > item['length']:
            raise TorrentError('Read exceeds selected file')
        position = item['offset'] + offset
        remaining = length
        generation = None
        tail_limit = min(4*1024*1024,max(4*self.meta.piece_length,item['length']//128))
        index_probe = offset > 0 and offset+length == item['length'] and length <= tail_limit
        if self.stream_file == file_index and not index_probe and length >= min(256*1024,item['length']):
            with self.lock:
                self.read_sequence += 1
                generation = self.window_generation = self.read_sequence
                self.readers[generation] = position
                self.reader_seen[generation] = time.monotonic()
        with self.lock:
            # Each connection is its own reader: when one of two probes closes,
            # the piece the other still waits for must not be given up.
            self.anonymous -= 1
            reader = generation or self.anonymous
        try:
            if generation is not None:
                # A new reader is a seek: retarget read-ahead now, so obsolete
                # pieces release their peers while the first piece downloads.
                if self.scheduler is not None:
                    self._left_behind(generation)
                self._window_at(position, generation)
            while remaining:
                index, inside = divmod(position, self.meta.piece_length)
                content = ((yield from self._awaited(index, heartbeat, generation, reader)) if heartbeat
                           else self.piece(index, reader=reader))
                size = min(remaining, len(content) - inside)
                final_index = (item['offset'] + item['length'] - 1) // self.meta.piece_length
                if generation is not None:
                    self._window_at(position+size,generation)
                elif self.stream_file is None:
                    self.prefetch(range(index + 1, min(index + 5, final_index + 1)))
                # A view, not a slice: no second copy of a piece that may be 16 MiB.
                yield memoryview(content)[inside:inside + size]
                position += size
                remaining -= size
        finally:
            restore = None
            if generation is not None:
                with self.lock:
                    self.readers.pop(generation,None)
                    self.reader_seen.pop(generation,None)
                    if generation == self.window_generation and self.readers:
                        # Kodi parks the previous connection during a seek without
                        # reading it. Only a reader that is really moving gets the
                        # window back; a parked one adopts it when it reads again.
                        latest = max(self.readers, key=lambda key: self.reader_seen.get(key, 0))
                        if time.monotonic() - self.reader_seen.get(latest, 0) <= self.PARKED_AFTER:
                            self.window_generation = latest
                            restore = (self.readers[latest], latest)
                if restore:
                    self._window_at(*restore)
