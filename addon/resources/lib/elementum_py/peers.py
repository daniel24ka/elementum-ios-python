"""Persistent BEP 3 TCP peers with metadata serving and verified piece reads."""

import socket
import random
import queue
import struct
import threading
import time
from collections import Counter

from .core import TorrentError, bdecode, bencode
from .discovery import compact_peers, handshake, read_message, send_message


class MissingPiece(TorrentError):
    pass


class PeerSession:
    def __init__(self, meta, address, cancel=None):
        self.meta = meta
        self.address = address
        self.cancel = cancel or threading.Event()
        self.lock = threading.Lock()
        self.busy = threading.Lock()
        self.channel = None
        self.reader = None
        self.received = queue.Queue(maxsize=128)
        self.send_lock = threading.Lock()
        self.wake = threading.Event()
        self.on_state = lambda: None
        self.last_error = ''
        self.choke_generation = 0
        self.previous_requests = set()
        self.available = None
        self.choked = True
        self.closed = False
        self.extension_ids = {}
        self.request_limit = 16
        self.new_peers = []
        self.pieces = 0
        self.bytes_received = 0
        self.elapsed = 0.001
        try:
            self.channel = socket.create_connection(address, timeout=3)
            deadline = time.monotonic() + 7
            reserved = handshake(self.channel, meta.info_hash, deadline)
            self._send(b'\x05' + bytes((len(meta.hashes) + 7) // 8))
            if reserved[5] & 0x10:
                extensions = {b'ut_metadata': 1}
                if not meta.private:
                    extensions[b'ut_pex'] = 2
                self._send(b'\x14\x00' + bencode({
                    b'm': extensions, b'metadata_size': len(meta.info_bytes),
                    b'v': b'ElementumPython/0.2', b'reqq': 32}))
            self._send(b'\x02')
            # A TCP handshake is not an unchoke. Real peers may wait for their
            # 10/30-second choking rotation before granting an upload slot.
            self.reader = threading.Thread(target=self._read, daemon=True)
            self.reader.start()
        except Exception:
            self.close()
            raise

    def _cancelled(self):
        if self.closed or self.cancel.is_set():
            raise TorrentError(self.last_error or 'Peer closed or cancelled')

    def _send(self, payload):
        with self.send_lock:
            send_message(self.channel, payload)

    def _read(self):
        try:
            while not self.closed and not self.cancel.is_set():
                message = read_message(self.channel, time.monotonic() + 180)
                if message and message[0] == 7:
                    if not 9 < len(message) <= 9 + 16384:
                        raise TorrentError('Invalid piece block length')
                    try:
                        self.received.put(message, timeout=1)
                    except queue.Full:
                        raise TorrentError('Peer exceeded the bounded block queue') from None
                else:
                    self._control(message)
                    self.on_state()
                self.wake.set()
        except (OSError, TorrentError, ValueError) as error:
            self.last_error = str(error)[:150]
        finally:
            self.close()

    def _control(self, message):
        if not message:
            return
        kind = message[0]
        if kind in (0, 1):
            if len(message) != 1:
                raise TorrentError('Malformed choke state')
            self.choked = kind == 0
            if self.choked:
                self.choke_generation += 1
        elif kind == 4:
            if len(message) != 5:
                raise TorrentError('Malformed HAVE message')
            index = struct.unpack('!I', message[1:])[0]
            if index >= len(self.meta.hashes):
                raise TorrentError('Invalid HAVE index')
            if self.available is not None:
                self.available.add(index)
        elif kind == 5:
            bits = message[1:]
            if len(bits) != (len(self.meta.hashes) + 7) // 8:
                raise TorrentError('Invalid bitfield size')
            unused = len(bits) * 8 - len(self.meta.hashes)
            if unused and bits[-1] & ((1 << unused) - 1):
                raise TorrentError('Nonzero padding bits')
            self.available = {index for index in range(len(self.meta.hashes))
                              if bits[index // 8] & (128 >> (index % 8))}
        elif kind == 20:
            if len(message) < 3:
                raise TorrentError('Malformed extended message')
            extension = message[1]
            if extension == 0:
                header = bdecode(message[2:], strict=False)
                if not isinstance(header, dict) or not isinstance(header.get(b'm', {}), dict):
                    raise TorrentError('Malformed extension handshake')
                self.extension_ids = header.get(b'm', {})
                request_limit = header.get(b'reqq')
                if type(request_limit) is int and request_limit > 0:
                    self.request_limit = min(16, request_limit)
            elif extension == 1:
                header, _ = bdecode(message[2:], prefix=True, strict=False)
                remote_id = self.extension_ids.get(b'ut_metadata', 0)
                if header.get(b'msg_type') == 0 and type(remote_id) is int and 1 <= remote_id <= 255:
                    index = header.get(b'piece')
                    count = (len(self.meta.info_bytes) + 16383) // 16384
                    if type(index) is int and 0 <= index < count:
                        reply = bencode({b'msg_type': 1, b'piece': index, b'total_size': len(self.meta.info_bytes)})
                        reply += self.meta.info_bytes[index * 16384:(index + 1) * 16384]
                    else:
                        reply = bencode({b'msg_type': 2, b'piece': index if type(index) is int else -1})
                    self._send(bytes((20, remote_id)) + reply)
            elif extension == 2 and not self.meta.private:
                header = bdecode(message[2:], strict=False)
                if isinstance(header, dict):
                    self.new_peers.extend(compact_peers(header.get(b'added', b'')))
                    self.new_peers = list(dict.fromkeys(self.new_peers))[:80]

    def fetch(self, index, timeout=12):
        deadline = time.monotonic() + timeout
        with self.lock:
            self._cancelled()
            if self.available is not None and index not in self.available:
                raise MissingPiece('Peer does not have requested piece')
            size = self.meta.piece_size(index)
            wanted = {offset: min(16384, size - offset) for offset in range(0, size, 16384)}
            pending = {}
            blocks = {}
            requested = set()
            choke_generation = self.choke_generation
            started = time.monotonic()
            try:
                while len(blocks) < len(wanted):
                    if time.monotonic() >= deadline:
                        raise TorrentError('Peer timed out waiting for an upload slot or requested blocks')
                    if self.cancel.is_set() or (self.closed and self.received.empty()):
                        self._cancelled()
                    if self.available is not None and index not in self.available:
                        raise MissingPiece('Peer does not have requested piece')
                    if choke_generation != self.choke_generation:
                        pending.clear()
                        choke_generation = self.choke_generation
                    if not self.choked and not self.closed:
                        for offset, length in wanted.items():
                            if len(pending) >= self.request_limit:
                                break
                            if offset not in blocks and offset not in pending:
                                self._send(struct.pack('!BIII', 6, index, offset, length))
                                pending[offset] = length
                                requested.add(offset)
                    try:
                        message = self.received.get(timeout=min(0.1, max(0.01, deadline - time.monotonic())))
                    except queue.Empty:
                        continue
                    piece_index, offset = struct.unpack('!II', message[1:9])
                    if piece_index != index:
                        if (piece_index, offset) in self.previous_requests:
                            continue  # A previously requested block may arrive after a choke.
                        raise TorrentError('Unrequested piece block')
                    if offset not in requested or len(message) - 9 != wanted.get(offset):
                        raise TorrentError('Unrequested piece block')
                    if offset not in blocks:
                        blocks[offset] = message[9:]
                    pending.pop(offset, None)
                content = self.meta.verify(index, b''.join(blocks[offset] for offset in sorted(wanted)))
                self.pieces += 1
                self.bytes_received += len(content)
                self.elapsed += max(0.001, time.monotonic() - started)
                self.previous_requests = {(index, offset) for offset in requested}
                try:
                    self._send(struct.pack('!BI', 4, index))
                except OSError:
                    pass  # Already verified bytes remain valid if the peer closes.
                return content
            except MissingPiece:
                raise
            except Exception:
                self.close()
                raise

    def close(self):
        self.closed = True
        if self.channel is not None:
            try:
                self.channel.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.channel.close()
        self.wake.set()
        self.on_state()
        if self.reader and self.reader is not threading.current_thread():
            self.reader.join(timeout=1)


class PeerPool:
    def __init__(self, meta, addresses, cancel=None, priority=()):
        self.meta = meta
        self.cancel = cancel or threading.Event()
        preferred = [address for address in dict.fromkeys(priority) if address in addresses][:8]
        remaining = [address for address in dict.fromkeys(addresses) if address not in preferred]
        random.SystemRandom().shuffle(remaining)
        # Retain the peer that just served verified metadata. Re-randomizing it
        # among hundreds of stale endpoints can exhaust startup before trying it.
        self.addresses = (preferred + remaining)[:200]
        self.queued = list(self.addresses)
        self.seen = set(self.addresses)
        self.sessions = []
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.connecting = 0
        self.ready = threading.Event()
        self.connectors = []
        self.closed = False
        self.errors = 0
        self.error_details = Counter()
        self.retired_bytes = 0
        self.refresher = None
        self.stopping = threading.Event()

    def prepare(self, timeout=15):
        queued = self.queued

        def worker():
            while not self.closed and not self.cancel.is_set():
                with self.condition:
                    self._prune_closed()
                    while ((not queued or sum(not peer.closed for peer in self.sessions) + self.connecting >= 8)
                           and not self.closed and not self.cancel.is_set()):
                        self.condition.wait(0.5)
                    if self.closed or self.cancel.is_set():
                        return
                    address = queued.pop(0)
                    self.connecting += 1
                try:
                    session = PeerSession(self.meta, address, self.cancel)
                    with self.lock:
                        if self.closed:
                            session.close()
                            return
                        self.sessions.append(session)
                        session.on_state = self._peer_changed
                        self.ready.set()
                except (OSError, TorrentError, ValueError) as error:
                    with self.condition:
                        self.errors += 1
                        self.error_details[type(error).__name__ + ': ' + str(error)[:100]] += 1
                finally:
                    with self.condition:
                        self.connecting -= 1
                        self.condition.notify_all()

        for _ in range(min(6, len(queued))):
            thread = threading.Thread(target=worker, daemon=True)
            self.connectors.append(thread)
            thread.start()
        deadline = time.monotonic() + timeout
        while not self.ready.wait(0.1):
            if self.cancel.is_set() or time.monotonic() >= deadline or not any(t.is_alive() for t in self.connectors):
                raise TorrentError('No usable TCP peers connected')

    def fetch(self, index, timeout=None):
        if timeout is None:
            timeout = min(90, max(45, 6 + self.meta.piece_size(index) / (256 * 1024)))
        deadline = time.monotonic() + timeout
        attempted = set()
        while time.monotonic() < deadline and not self.closed and not self.cancel.is_set():
            with self.lock:
                available = [peer for peer in self.sessions if not peer.closed and peer not in attempted]
                missing = [peer for peer in available if getattr(peer, 'available', None) is not None
                           and index not in peer.available]
                available = [peer for peer in available if peer not in missing]
                available.sort(key=lambda peer: (peer.busy.locked(), getattr(peer, 'choked', False),
                                                 -(peer.bytes_received / peer.elapsed)))
            for peer in missing:
                peer.close()
                attempted.add(peer)
            if not available:
                with self.lock:
                    if not self.connecting and not self.queued and not any(not peer.closed for peer in self.sessions):
                        break
                self.cancel.wait(0.05)
                continue
            if all(getattr(peer, 'choked', False) for peer in available):
                self.cancel.wait(0.05)
                continue  # Readers keep processing all peers while upload slots rotate.
            peer = available[0]
            if not peer.busy.acquire(blocking=False):
                self.cancel.wait(0.02)
                continue
            attempted.add(peer)
            try:
                return peer.fetch(index, timeout=min(45, max(0.1, deadline - time.monotonic())))
            except (OSError, TorrentError) as error:
                # Replace peers that cannot supply the selected file instead of
                # keeping eight healthy connections to the wrong pieces forever.
                peer.close()
                with self.condition:
                    self.errors += 1
                    self.error_details[type(error).__name__ + ': ' + str(error)[:100]] += 1
                    self.condition.notify_all()
            finally:
                peer.busy.release()
        raise TorrentError('No connected peer supplied a valid requested piece')

    def _peer_changed(self):
        with self.condition:
            self._prune_closed()
            for peer in self.sessions:
                self._enqueue(getattr(peer, 'new_peers', []))
            self.condition.notify_all()

    def _prune_closed(self):
        retained = []
        for peer in self.sessions:
            if peer.closed and not peer.busy.locked():
                self.retired_bytes += peer.bytes_received
            else:
                retained.append(peer)
        self.sessions = retained

    def _enqueue(self, addresses):
        if self.closed:
            return
        for address in addresses:
            if address not in self.seen and len(self.seen) < 400:
                self.seen.add(address)
                self.queued.append(address)

    def add_peers(self, addresses):
        with self.condition:
            self._enqueue(addresses)
            self.condition.notify_all()

    def refresh(self, discover):
        def work():
            delay = 1
            while not self.stopping.wait(delay):
                if self.closed or self.cancel.is_set():
                    return
                try:
                    self.add_peers(discover())
                except (OSError, TorrentError, ValueError):
                    pass
                delay = 60
        self.refresher = threading.Thread(target=work, daemon=True)
        self.refresher.start()

    def stats(self):
        with self.lock:
            return {'connected_peers': sum(not peer.closed for peer in self.sessions),
                    'unchoked_peers': sum(not peer.closed and not getattr(peer, 'choked', False) for peer in self.sessions),
                    'queued_peers': len(self.queued),
                    'peer_bytes_received': self.retired_bytes + sum(peer.bytes_received for peer in self.sessions),
                    'peer_errors': self.errors, 'peer_error_details': dict(self.error_details.most_common(6)),
                    'connecting_peers': self.connecting}

    def close(self):
        self.closed = True
        self.stopping.set()
        with self.condition:
            sessions = list(self.sessions)
            self.condition.notify_all()
        for peer in sessions:
            peer.close()
        deadline = time.monotonic() + 10
        for thread in self.connectors + ([self.refresher] if self.refresher else []):
            if thread is not threading.current_thread():
                thread.join(timeout=max(0, deadline - time.monotonic()))
