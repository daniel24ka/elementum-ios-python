"""Persistent BEP 3 TCP peers with metadata serving and verified piece reads."""

import socket
import random
import struct
import threading
import time

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
        self.available = None
        self.choked = True
        self.closed = False
        self.extension_ids = {}
        self.new_peers = []
        self.pieces = 0
        self.bytes_received = 0
        self.elapsed = 0.001
        try:
            self.channel = socket.create_connection(address, timeout=3)
            deadline = time.monotonic() + 7
            reserved = handshake(self.channel, meta.info_hash, deadline)
            send_message(self.channel, b'\x05' + bytes((len(meta.hashes) + 7) // 8))
            if reserved[5] & 0x10:
                extensions = {b'ut_metadata': 1}
                if not meta.private:
                    extensions[b'ut_pex'] = 2
                send_message(self.channel, b'\x14\x00' + bencode({
                    b'm': extensions, b'metadata_size': len(meta.info_bytes),
                    b'v': b'ElementumPython/0.2', b'reqq': 32}))
            send_message(self.channel, b'\x02')
            while self.choked:
                self._control(read_message(self.channel, deadline))
                self._cancelled()
        except Exception:
            self.close()
            raise

    def _cancelled(self):
        if self.closed or self.cancel.is_set():
            raise TorrentError('Peer closed or cancelled')

    def _control(self, message):
        if not message:
            return
        kind = message[0]
        if kind in (0, 1):
            if len(message) != 1:
                raise TorrentError('Malformed choke state')
            self.choked = kind == 0
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
                header = bdecode(message[2:])
                if not isinstance(header, dict) or not isinstance(header.get(b'm', {}), dict):
                    raise TorrentError('Malformed extension handshake')
                self.extension_ids = header.get(b'm', {})
            elif extension == 1:
                header, _ = bdecode(message[2:], prefix=True)
                remote_id = self.extension_ids.get(b'ut_metadata', 0)
                if header.get(b'msg_type') == 0 and type(remote_id) is int and 1 <= remote_id <= 255:
                    index = header.get(b'piece')
                    count = (len(self.meta.info_bytes) + 16383) // 16384
                    if type(index) is int and 0 <= index < count:
                        reply = bencode({b'msg_type': 1, b'piece': index, b'total_size': len(self.meta.info_bytes)})
                        reply += self.meta.info_bytes[index * 16384:(index + 1) * 16384]
                    else:
                        reply = bencode({b'msg_type': 2, b'piece': index if type(index) is int else -1})
                    send_message(self.channel, bytes((20, remote_id)) + reply)
            elif extension == 2 and not self.meta.private:
                header = bdecode(message[2:])
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
            started = time.monotonic()
            try:
                while len(blocks) < len(wanted):
                    self._cancelled()
                    if self.available is not None and index not in self.available:
                        raise MissingPiece('Peer does not have requested piece')
                    if not self.choked:
                        for offset, length in wanted.items():
                            if len(pending) >= 16:
                                break
                            if offset not in blocks and offset not in pending:
                                send_message(self.channel, struct.pack('!BIII', 6, index, offset, length))
                                pending[offset] = length
                    message = read_message(self.channel, deadline)
                    if message and message[0] == 7:
                        if len(message) < 9:
                            raise TorrentError('Truncated piece response')
                        piece_index, offset = struct.unpack('!II', message[1:9])
                        if piece_index != index or offset not in pending or len(message) - 9 != pending[offset]:
                            raise TorrentError('Unrequested piece block')
                        blocks[offset] = message[9:]
                        del pending[offset]
                    else:
                        was_choked = self.choked
                        self._control(message)
                        if not was_choked and self.choked:
                            pending.clear()
                content = self.meta.verify(index, b''.join(blocks[offset] for offset in sorted(wanted)))
                self.pieces += 1
                self.bytes_received += len(content)
                self.elapsed += max(0.001, time.monotonic() - started)
                send_message(self.channel, struct.pack('!BI', 4, index))
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


class PeerPool:
    def __init__(self, meta, addresses, cancel=None):
        self.meta = meta
        self.cancel = cancel or threading.Event()
        self.addresses = list(dict.fromkeys(addresses))[:200]
        random.SystemRandom().shuffle(self.addresses)
        self.addresses = self.addresses[:100]
        self.sessions = []
        self.lock = threading.Lock()
        self.ready = threading.Event()
        self.connectors = []
        self.closed = False
        self.errors = 0

    def prepare(self, timeout=15):
        queued = list(self.addresses)

        def worker():
            while not self.closed and not self.cancel.is_set():
                with self.lock:
                    healthy = sum(not peer.closed for peer in self.sessions)
                    if not queued or healthy >= 8:
                        return
                    address = queued.pop(0)
                try:
                    session = PeerSession(self.meta, address, self.cancel)
                    with self.lock:
                        if self.closed:
                            session.close()
                            return
                        self.sessions.append(session)
                        self.ready.set()
                except (OSError, TorrentError, ValueError):
                    self.errors += 1

        for _ in range(min(6, len(queued))):
            thread = threading.Thread(target=worker, daemon=True)
            self.connectors.append(thread)
            thread.start()
        deadline = time.monotonic() + timeout
        while not self.ready.wait(0.1):
            if self.cancel.is_set() or time.monotonic() >= deadline or not any(t.is_alive() for t in self.connectors):
                raise TorrentError('No usable TCP peers connected')

    def fetch(self, index, timeout=20):
        deadline = time.monotonic() + timeout
        attempted = set()
        while time.monotonic() < deadline and not self.closed and not self.cancel.is_set():
            with self.lock:
                available = [peer for peer in self.sessions if not peer.closed and peer not in attempted]
                available.sort(key=lambda peer: (peer.busy.locked(), -(peer.bytes_received / peer.elapsed)))
            if not available:
                if not any(t.is_alive() for t in self.connectors):
                    break
                self.cancel.wait(0.05)
                continue
            peer = available[0]
            if not peer.busy.acquire(blocking=False):
                self.cancel.wait(0.02)
                continue
            attempted.add(peer)
            try:
                return peer.fetch(index, timeout=min(10, max(0.1, deadline - time.monotonic())))
            except (OSError, TorrentError):
                self.errors += 1
            finally:
                peer.busy.release()
        raise TorrentError('No connected peer supplied a valid requested piece')

    def stats(self):
        with self.lock:
            return {'connected_peers': sum(not peer.closed for peer in self.sessions),
                    'peer_bytes_received': sum(peer.bytes_received for peer in self.sessions),
                    'peer_errors': self.errors}

    def close(self):
        self.closed = True
        with self.lock:
            sessions = list(self.sessions)
        for peer in sessions:
            peer.close()
        for thread in self.connectors:
            thread.join(timeout=10)
