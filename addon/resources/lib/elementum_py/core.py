"""Validated v1 metainfo, piece integrity and a bounded verified-piece cache."""

import hashlib
import concurrent.futures
import os
import socket
import struct
import threading
import time
from collections import OrderedDict
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen


class TorrentError(Exception):
    pass


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
    """Bound memory and share verified pieces across HTTP range requests."""
    def __init__(self, meta, peers=(), max_cache_bytes=16 * 1024 * 1024,
                 peer_fetcher=None, cache_dir=None, allow_webseeds=True, cancel=None):
        self.meta = meta
        self.peers = tuple(peers)
        self.limit = max(1, max_cache_bytes // meta.piece_length)
        self.cache = OrderedDict()
        self.lock = threading.Lock()
        self.peer_fetcher = peer_fetcher
        self.cache_dir = cache_dir
        self.allow_webseeds = allow_webseeds
        self.cancel = cancel or threading.Event()
        self.inflight = {}
        self.verified_pieces = 0
        self.last_source = None
        self.disk_hits = 0
        self.prefetch_executor = None
        self.prefetch_futures = {}
        self.prefetch_workers = max(1, min(3, max_cache_bytes // meta.piece_length))
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)

    def piece(self, index):
        self.meta.piece_size(index)
        if self.cancel.is_set():
            raise TorrentError('Cancelled')
        with self.lock:
            if index in self.cache:
                self.cache.move_to_end(index)
                return self.cache[index]
            if index in self.inflight:
                ready = self.inflight[index]
                owner = False
            else:
                ready = threading.Event()
                self.inflight[index] = ready
                owner = True
        if not owner:
            while not ready.wait(0.1):
                if self.cancel.is_set():
                    raise TorrentError('Cancelled')
            with self.lock:
                if index in self.cache:
                    self.cache.move_to_end(index)
                    return self.cache[index]
            return self.piece(index)
        try:
            errors = []
            content = None
            if self.cache_dir:
                filename = os.path.join(self.cache_dir, str(index) + '.piece')
                try:
                    with open(filename, 'rb') as handle:
                        content = self.meta.verify(index, handle.read(self.meta.piece_size(index) + 1))
                    self.disk_hits += 1
                    self.last_source = 'Verified disk cache'
                except (OSError, TorrentError) as error:
                    content = None
            if content is None and self.peer_fetcher is not None:
                try:
                    content = self.meta.verify(index, self.peer_fetcher(index))
                    self.last_source = 'TCP peer'
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
                raise TorrentError('No usable piece source: ' + '; '.join(errors[-2:]))
            if self.cancel.is_set():
                raise TorrentError('Cancelled')
            if self.cache_dir:
                filename = os.path.join(self.cache_dir, str(index) + '.piece')
                temporary = filename + '.tmp'
                with open(temporary, 'wb') as handle:
                    handle.write(content)
                os.replace(temporary, filename)
            with self.lock:
                self.cache[index] = content
                while len(self.cache) > self.limit:
                    self.cache.popitem(last=False)
                self.verified_pieces += 1
            return content
        finally:
            with self.lock:
                self.inflight.pop(index, None)
                ready.set()

    def prefetch(self, indexes):
        with self.lock:
            if self.cancel.is_set():
                return
            if self.prefetch_executor is None:
                self.prefetch_executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.prefetch_workers)
            self.prefetch_futures = {index: future for index, future in self.prefetch_futures.items() if not future.done()}
            for index in indexes:
                if (0 <= index < len(self.meta.hashes) and index not in self.cache and
                        index not in self.prefetch_futures and len(self.prefetch_futures) < self.prefetch_workers):
                    self.prefetch_futures[index] = self.prefetch_executor.submit(self._prefetch, index)

    def _prefetch(self, index):
        # A completed Future must not retain another large piece outside the LRU.
        # Foreground reads retry failed prefetches through the normal source path.
        try:
            self.piece(index)
        except (TorrentError, OSError):
            pass

    def close(self):
        self.cancel.set()
        if self.prefetch_executor is not None:
            for future in self.prefetch_futures.values():
                future.cancel()
            self.prefetch_executor.shutdown(wait=True)

    def read(self, file_index, offset, length):
        item = self.meta.files[file_index]
        if offset < 0 or length < 0 or offset + length > item['length']:
            raise TorrentError('Read exceeds selected file')
        position = item['offset'] + offset
        remaining = length
        while remaining:
            index, inside = divmod(position, self.meta.piece_length)
            content = self.piece(index)
            size = min(remaining, len(content) - inside)
            final_index = (item['offset'] + item['length'] - 1) // self.meta.piece_length
            self.prefetch(range(index + 1, min(index + 5, final_index + 1)))
            yield content[inside:inside + size]
            position += size
            remaining -= size
