"""Block-level BEP 3/6 swarm for streaming.

Every unchoked peer works on the most urgent wanted piece at the same time.
Requests are pipelined by the measured rate of each peer, a late urgent block is
duplicated on a faster peer, and a silent peer is snubbed instead of trusted.
A piece is returned only after SHA-1 verification, and a peer that sent a bad
block is identified by comparing its blocks with the verified piece.

Scheduling ideas follow libtorrent's time-critical pieces (peer queue time,
busy blocks, snubbing) and WebTorrent's speed ranking; no code was copied.
"""

import hashlib
import math
import random
import socket
import struct
import threading
import time
import zlib
from collections import Counter

from .core import Abandoned, TorrentError, bdecode, bencode
from .discovery import compact_peers, handshake

BLOCK = 16384
MAX_CONNECTIONS = 20
CONNECT_TIMEOUT = 3
HANDSHAKE_TIMEOUT = 7
IDLE_TIMEOUT = 150          # Peers send a keep-alive every two minutes.
KEEPALIVE = 60
QUEUE_SECONDS = 1.5         # Outstanding work kept on a peer, in seconds of its rate.
START_DEPTH = 4
MAX_DEPTH = 128
DEFAULT_REQUEST_LIMIT = 16  # For peers that never announce "reqq".
NOMINAL_RATE = 64 * 1024    # Prior for a peer that has not delivered a block yet.
SEND_PATIENCE = 1.0         # Longest a scheduler thread waits for one peer's socket.
SECOND_CHANCE = 10          # A quiet peer with nothing outstanding is tried again after this.
CHOKED_EVICTION = 60
SNUB_EVICTION = 30
USELESS_EVICTION = 15

_REQUEST = struct.Struct('!IBIII')
_PIECE = struct.Struct('!II')
_LENGTH = struct.Struct('!I')
clock = time.perf_counter


class MissingPiece(TorrentError):
    pass


def _frame(payload):
    return _LENGTH.pack(len(payload)) + payload


class PeerSession:
    """One TCP connection: wire parsing here, every decision in the Swarm."""

    def __init__(self, meta, address, cancel=None, owner=None):
        self.meta = meta
        self.address = address
        self.cancel = cancel or threading.Event()
        self.standalone = owner is None
        self.owner = Swarm(meta, self.cancel) if owner is None else owner
        self.send_lock = threading.Lock()
        self.channel = None
        self.reader = None
        self.closed = False
        self.detached = False
        self.last_error = ''
        self.choked = True
        self.fast = False
        self.known = False          # A bitfield or HAVE has described this peer.
        self.complete = False
        self.bits = None
        self.have = 0
        self.allowed_fast = set()
        self.extension_ids = {}
        self.request_limit = DEFAULT_REQUEST_LIMIT
        self.new_peers = []
        self.pex_flags = Counter()
        # Scheduler state below is guarded by owner.lock.
        self.outstanding = {}       # (index, begin) -> [length, sent, expected arrival]
        self.queued_bytes = 0
        self.stale = {}             # Requests released early; one late block each is tolerated.
        self.depth = START_DEPTH
        self.rate = 0.0
        self.sample_bytes = 0
        self.sample_time = 0.0
        self.snubbed = False
        self.snubbed_since = 0.0
        self.suspect = False        # Went quiet in mid-transfer; not yet proven silent.
        self.quiet_since = 0.0
        self.chances = 0            # Second chances used since its last delivered block.
        self.replies = 0            # Answers sent to this peer's own requests.
        self.rejects = 0
        self.blocks = 0
        self.pieces = 0
        self.bytes_received = 0
        self.wasted_bytes = 0
        self.elapsed = 0.001
        now = clock()
        self.connected = self.choked_since = self.last_sent = now
        self.last_block = self.busy_since = self.useless_since = 0.0
        try:
            self.channel = socket.create_connection(address, timeout=CONNECT_TIMEOUT)
            try:
                # Requests are 17 bytes; Nagle would hold them for a delayed ACK.
                self.channel.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            reserved = handshake(self.channel, meta.info_hash, time.monotonic() + HANDSHAKE_TIMEOUT, fast=True)
            self.fast = bool(reserved[7] & 0x04)
            hello = bytearray()
            if self.fast:
                hello += _frame(b'\x0f')  # HAVE_NONE: exactly one availability message is mandatory.
            if reserved[5] & 0x10:
                extensions = {b'ut_metadata': 1}
                if not meta.private:
                    extensions[b'ut_pex'] = 2
                hello += _frame(b'\x14\x00' + bencode({
                    b'm': extensions, b'metadata_size': len(meta.info_bytes),
                    b'v': b'ElementumPython/0.3', b'reqq': 32}))
            hello += _frame(b'\x02')
            # A TCP handshake is not an unchoke. Real peers may wait for their
            # 10/30-second choking rotation before granting an upload slot.
            self.channel.sendall(hello)
            self.channel.settimeout(IDLE_TIMEOUT)
            self.owner.attach(self)
            self.reader = threading.Thread(target=self._read, daemon=True)
            self.reader.start()
        except Exception:
            self.close()
            raise

    def has(self, index):
        return self.complete or bool(self.bits is not None and self.bits[index >> 3] & (128 >> (index & 7)))

    def estimate(self):
        """Bytes per second this peer is expected to deliver."""
        if self.snubbed:
            return 1024.0
        return self.rate or self.owner.prior()

    def timeout(self):
        """Silence tolerated while requests are outstanding (3 to 20 seconds)."""
        if not self.blocks:
            return 10.0
        return min(20.0, max(3.0, 2.0 + 8.0 * BLOCK / max(self.rate, 1.0)))

    def _send(self, data):
        # A peer that stopped reading its socket blocks whoever is sending to it.
        # Any other thread gives the peer up after a second instead of queuing
        # behind that send; the caller then closes the session.
        if not self.send_lock.acquire(timeout=SEND_PATIENCE):
            raise OSError('Peer does not read its socket')
        try:
            self.channel.sendall(data)
            self.last_sent = clock()
        finally:
            self.send_lock.release()

    def _read(self):
        limit = max(BLOCK + 9, (len(self.meta.hashes) + 7) // 8 + 1, 32768)
        buffer = bytearray(limit + 4 + BLOCK)
        view = memoryview(buffer)
        start = end = 0
        try:
            while not self.closed and not self.cancel.is_set():
                if end - start >= 4:
                    length = _LENGTH.unpack_from(buffer, start)[0]
                    if length > limit:
                        raise TorrentError('Peer message too large')
                    if end - start >= 4 + length:
                        if length:
                            self._message(view[start + 4:start + 4 + length])
                        start += 4 + length
                        if start == end:
                            start = end = 0
                        continue
                if start and len(buffer) - end < BLOCK:
                    remaining = end - start
                    buffer[0:remaining] = buffer[start:end]
                    start, end = 0, remaining
                received = self.channel.recv_into(view[end:])
                if not received:
                    raise TorrentError('Peer disconnected')
                end += received
        except (OSError, TorrentError, ValueError, struct.error) as error:
            if not self.last_error:
                self.last_error = (str(error) or type(error).__name__)[:150]
        finally:
            self.close()

    def _message(self, payload):
        kind = payload[0]
        if kind == 7:
            if not 9 < len(payload) <= 9 + BLOCK:
                raise TorrentError('Invalid piece block length')
            index, begin = _PIECE.unpack_from(payload, 1)
            self.owner.block(self, index, begin, payload[9:])
            return
        count = len(self.meta.hashes)
        if kind in (0, 1):
            if len(payload) != 1:
                raise TorrentError('Malformed choke state')
            self.owner.choke(self, kind == 0)
        elif kind == 4:
            if len(payload) != 5:
                raise TorrentError('Malformed HAVE message')
            index = _LENGTH.unpack_from(payload, 1)[0]
            if index >= count:
                raise TorrentError('Invalid HAVE index')
            self.known = True
            if not self.has(index):
                if self.bits is None:
                    self.bits = bytearray((count + 7) // 8)
                self.bits[index >> 3] |= 128 >> (index & 7)
                self.have += 1
                self.complete = self.have == count
                self.owner.changed(self)
        elif kind == 5:
            bits = bytearray(payload[1:])
            if len(bits) != (count + 7) // 8:
                raise TorrentError('Invalid bitfield size')
            unused = len(bits) * 8 - count
            if unused and bits[-1] & ((1 << unused) - 1):
                raise TorrentError('Nonzero padding bits')
            self.bits, self.known = bits, True
            self.have = bin(int.from_bytes(bits, 'big')).count('1')
            self.complete = self.have == count
            self.owner.changed(self)
        elif kind == 20:
            self._extended(payload)
        elif self.fast:
            if kind == 14 and len(payload) == 1:
                self.known = self.complete = True
                self.have = count
                self.owner.changed(self)
            elif kind == 15 and len(payload) == 1:
                self.known, self.complete, self.bits, self.have = True, False, None, 0
            elif kind == 16 and len(payload) == 13:
                index, begin = _PIECE.unpack_from(payload, 1)
                self.owner.rejected(self, index, begin)
            elif kind == 17 and len(payload) == 5:
                index = _LENGTH.unpack_from(payload, 1)[0]
                if index < count and len(self.allowed_fast) < 64:
                    self.allowed_fast.add(index)
                    self.owner.changed(self)
            elif kind == 6 and len(payload) == 13 and self.replies < 64:
                # This engine does not upload; BEP 6 requires an explicit answer.
                # Bounded: a flood of requests must not turn into a flood of replies.
                self.replies += 1
                self._send(_frame(b'\x10' + bytes(payload[1:])))
        # Interested, cancel, port, suggest and unknown messages need no action.

    def _extended(self, payload):
        if len(payload) < 3:
            raise TorrentError('Malformed extended message')
        extension = payload[1]
        body = bytes(payload[2:])
        if extension == 0:
            header = bdecode(body, strict=False)
            if not isinstance(header, dict) or not isinstance(header.get(b'm', {}), dict):
                raise TorrentError('Malformed extension handshake')
            self.extension_ids = header.get(b'm', {})
            request_limit = header.get(b'reqq')
            if type(request_limit) is int and request_limit > 0:
                self.request_limit = min(MAX_DEPTH, request_limit)
        elif extension == 1:
            header, _ = bdecode(body, prefix=True, strict=False)
            remote_id = self.extension_ids.get(b'ut_metadata', 0)
            count = (len(self.meta.info_bytes) + 16383) // 16384
            if (isinstance(header, dict) and header.get(b'msg_type') == 0
                    and type(remote_id) is int and 1 <= remote_id <= 255 and self.replies < 2 * count + 64):
                self.replies += 1
                index = header.get(b'piece')
                if type(index) is int and 0 <= index < count:
                    reply = bencode({b'msg_type': 1, b'piece': index, b'total_size': len(self.meta.info_bytes)})
                    reply += self.meta.info_bytes[index * 16384:(index + 1) * 16384]
                else:
                    reply = bencode({b'msg_type': 2, b'piece': index if type(index) is int else -1})
                self._send(_frame(bytes((20, remote_id)) + reply))
        elif extension == 2 and not self.meta.private:
            header = bdecode(body, strict=False)
            if not isinstance(header, dict):
                return
            added = compact_peers(header.get(b'added', b''))
            flags = header.get(b'added.f', b'')
            if isinstance(flags, bytes) and len(flags) * 6 == len(header.get(b'added', b'')):
                self.pex_flags['encryption_preferred'] += sum(1 for flag in flags if flag & 0x01)
                self.pex_flags['utp'] += sum(1 for flag in flags if flag & 0x04)
                self.pex_flags['announced'] += len(flags)
            if isinstance(header.get(b'added6'), bytes):
                try:
                    added += compact_peers(header[b'added6'], ipv6=True)
                except TorrentError:
                    pass
            self.new_peers = list(dict.fromkeys(self.new_peers + added))[:80]
            self.owner.changed(self)

    def fetch(self, index, timeout=12):
        """Fetch one verified piece from this peer alone."""
        try:
            return self.owner.fetch(index, timeout=timeout, pinned=self)
        except MissingPiece:
            raise
        except Exception:
            if self.standalone:
                self.close()
            raise

    def close(self, reason=''):
        if reason and not self.last_error:
            self.last_error = reason[:150]
        already = self.closed
        self.closed = True
        channel = self.channel
        if channel is not None and not already:
            try:
                channel.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                channel.close()
            except OSError:
                pass
        self.owner.detach(self)

    def join(self, timeout=1):
        if self.reader and self.reader is not threading.current_thread():
            self.reader.join(timeout=max(0, timeout))


class PieceJob:
    __slots__ = ('index', 'size', 'count', 'buffer', 'state', 'holders', 'done', 'authors', 'urgent',
                 'created', 'finished', 'abandoned', 'pinned', 'exclusive', 'cursor', 'failures', 'suspects',
                 'result', 'error', 'settled', 'hedge_after')

    def __init__(self, meta, index, urgent, pinned=None):
        self.index = index
        self.size = meta.piece_size(index)
        self.count = (self.size + BLOCK - 1) // BLOCK
        self.buffer = bytearray(self.size)
        self.state = bytearray(self.count)   # 0 missing, 1 requested, 2 received
        self.holders = {}                    # block -> {peer: expected arrival}
        self.authors = [None] * self.count   # Which peer delivered each accepted block.
        self.done = 0
        self.cursor = 0
        self.urgent = urgent
        self.pinned = pinned                 # The caller asked for this one peer only.
        self.exclusive = pinned              # The only peer allowed to deliver blocks right now.
        self.created = clock()
        self.finished = threading.Event()    # All blocks received, or abandoned.
        self.settled = threading.Event()     # The owner has a result or an error.
        self.abandoned = False
        self.failures = 0
        self.suspects = None                 # (block checksums, block authors) of a failed attempt
        self.result = self.error = None
        self.hedge_after = 0.0

    def length(self, block):
        return min(BLOCK, self.size - block * BLOCK)

    def reset(self):
        self.buffer = bytearray(self.size)
        self.state = bytearray(self.count)
        self.holders = {}
        self.authors = [None] * self.count
        self.done = self.cursor = 0
        self.finished.clear()


class Swarm:
    """Decides which peer downloads which block. Owns no thread of its own."""

    def __init__(self, meta, cancel=None, lock=None, pending=None, notify=None):
        self.meta = meta
        self.cancel = cancel or threading.Event()
        self.lock = lock or threading.RLock()
        self.pending = pending or (lambda: False)   # True while more peers may still connect.
        self.notify = notify or (lambda peer: None)
        self.peers = []
        self.jobs = {}
        self.order = []
        self.banned = set()
        self.counters = Counter()
        self.retired_bytes = 0
        self.last_tick = 0.0
        self.stopped = False
        self.hints = set()          # Pieces a reader asked for before their download began.
        self.focus = 0              # Piece under the active reader; read-ahead starts here.

    # ------------------------------------------------------------ peer events
    def attach(self, peer):
        with self.lock:
            self.peers.append(peer)

    def detach(self, peer):
        outbox = {}
        with self.lock:
            if peer.detached:
                return
            peer.detached = True
            for key in list(peer.outstanding):
                self._release(peer, key, stale=False)
            if peer in self.peers:
                self.peers.remove(peer)
                self.retired_bytes += peer.bytes_received
            if not self.stopped:
                self._assign(self.peers, clock(), outbox)
        self._flush(outbox)
        self.notify(peer)

    def choke(self, peer, choked):
        outbox = {}
        now = clock()
        with self.lock:
            peer.choked = choked
            peer.rejects = 0
            if choked:
                peer.choked_since = now
                if not peer.fast:
                    # Without BEP 6 a choke silently drops every queued request.
                    for key in list(peer.outstanding):
                        self._release(peer, key)
                self._assign(self.peers, now, outbox)
            else:
                # An upload slot is a fresh start: whatever it did before the choke,
                # the peer has just said that it will serve again.
                peer.busy_since = now
                peer.suspect = False
                if peer.snubbed:
                    peer.snubbed = False
                    peer.depth = START_DEPTH
                self._assign([peer], now, outbox)
        self._flush(outbox)
        self.notify(peer)

    def changed(self, peer):
        outbox = {}
        with self.lock:
            self._assign([peer], clock(), outbox)
        self._flush(outbox)
        self.notify(peer)

    def rejected(self, peer, index, begin):
        outbox = {}
        now = clock()
        with self.lock:
            known = (index, begin) in peer.outstanding
            self._release(peer, (index, begin), stale=False)
            if known and not peer.choked:
                # A choking BEP 6 peer must reject every queued request; only
                # rejects from a peer that keeps us unchoked say something is wrong.
                peer.rejects += 1
                if peer.rejects >= 8 and not peer.snubbed:
                    self._snub(peer, now, outbox)
            self._assign([other for other in self.peers if other is not peer], now, outbox)
        self._flush(outbox)

    def block(self, peer, index, begin, data):
        """Accept one block from a reader thread; raises on unrequested data."""
        outbox = {}
        now = clock()
        size = len(data)
        key = (index, begin)
        with self.lock:
            request = peer.outstanding.pop(key, None)
            if request is not None:
                peer.queued_bytes -= request[0]
                expected = request[0]
            elif key in peer.stale:
                expected = peer.stale.pop(key)
            else:
                raise TorrentError('Unrequested piece block')
            if size != expected:
                raise TorrentError('Unrequested piece block')
            since = max(peer.last_block, peer.busy_since)
            if since:
                peer.sample_time += now - since
            peer.sample_bytes += size
            peer.last_block = now
            peer.blocks += 1
            peer.bytes_received += size
            peer.rejects = peer.chances = 0
            peer.suspect = False
            if peer.snubbed:
                peer.snubbed = False
                peer.depth = START_DEPTH
            self._measure(peer)
            job = self.jobs.get(index)
            block = begin // BLOCK
            if (job is not None and not job.abandoned and begin % BLOCK == 0 and job.state[block] != 2
                    and (job.exclusive is None or job.exclusive is peer)):
                job.buffer[begin:begin + size] = data
                job.state[block] = 2
                job.authors[block] = peer
                job.done += 1
                holders = job.holders.pop(block, None)
                if holders:
                    for other in holders:
                        if other is not peer:
                            self._cancel(other, key, outbox)
                if job.done == job.count:
                    job.finished.set()
            else:
                peer.wasted_bytes += size
                self.counters['wasted_bytes'] += size
                holders = job.holders.get(block) if job is not None else None
                if holders and peer in holders:
                    del holders[peer]
                    if not holders:
                        del job.holders[block]
                        if job.state[block] == 1:
                            job.state[block] = 0
                            job.cursor = min(job.cursor, block)
            self._assign([peer], now, outbox)
        self._flush(outbox)

    def _measure(self, peer):
        if peer.sample_time < 0.3:
            if not peer.rate:
                peer.depth = min(peer.depth + 1, self._limit(peer))   # Slow start.
            return
        sample = peer.sample_bytes / peer.sample_time
        weight = 1.0 - math.exp(-peer.sample_time / 2.0)
        peer.rate = sample if not peer.rate else peer.rate + weight * (sample - peer.rate)
        peer.sample_bytes, peer.sample_time = 0, 0.0
        peer.depth = max(2, min(self._limit(peer), int(math.ceil(peer.rate * QUEUE_SECONDS / BLOCK))))

    @staticmethod
    def _limit(peer):
        return max(1, min(MAX_DEPTH, peer.request_limit))

    def prior(self):
        rates = [peer.rate for peer in self.peers if peer.rate]
        return sum(rates) / len(rates) if rates else NOMINAL_RATE

    # ------------------------------------------------------------ bookkeeping
    def _release(self, peer, key, stale=True):
        request = peer.outstanding.pop(key, None)
        if request is None:
            return
        peer.queued_bytes -= request[0]
        if stale:
            peer.stale[key] = request[0]
            if len(peer.stale) > 512:
                del peer.stale[next(iter(peer.stale))]
        job = self.jobs.get(key[0])
        if job is None:
            return
        block = key[1] // BLOCK
        holders = job.holders.get(block)
        if holders is None:
            return
        holders.pop(peer, None)
        if not holders:
            del job.holders[block]
            if job.state[block] == 1:
                job.state[block] = 0
                if block < job.cursor:
                    job.cursor = block

    def _cancel(self, peer, key, outbox):
        request = peer.outstanding.get(key)
        if request is None:
            return
        length = request[0]
        self._release(peer, key)
        if not peer.closed:
            outbox.setdefault(peer, bytearray()).extend(_REQUEST.pack(13, 8, key[0], key[1], length))

    def _request(self, peer, job, block, now, outbox):
        length = job.length(block)
        key = (job.index, block * BLOCK)
        if not peer.outstanding:
            peer.busy_since = now
        expected = now + (peer.queued_bytes + length) / peer.estimate()
        peer.outstanding[key] = [length, now, expected]
        peer.queued_bytes += length
        job.holders.setdefault(block, {})[peer] = expected
        if job.state[block] == 0:
            job.state[block] = 1
        outbox.setdefault(peer, bytearray()).extend(_REQUEST.pack(13, 6, job.index, key[1], length))

    def _flush(self, outbox):
        for peer, data in outbox.items():
            if data and not peer.closed:
                try:
                    peer._send(bytes(data))
                except OSError as error:
                    peer.close(str(error))

    def _reorder(self):
        # Awaited pieces first; then read-ahead from the reader forwards; pieces
        # behind the reader (left over from before a seek) come last.
        focus = self.focus
        self.order = sorted(self.jobs.values(), key=lambda job: (not job.urgent, job.index < focus, job.index))

    # ------------------------------------------------------------ assignment
    def _assign(self, peers, now, outbox):
        if not self.order:
            return
        if len(peers) > 1:
            # The fastest peers choose first, so urgent blocks land on short queues.
            peers = sorted(peers, key=lambda peer: -peer.estimate())
        if len(peers) > 1:   # A full pass (timer, choke, new piece), not one block arriving.
            for job in self.order:
                if not job.urgent:
                    break
                if job.exclusive is None and job.done < job.count:
                    self._reclaim(job)
        waiting = any(job.urgent and job.cursor < job.count for job in self.order)
        for peer in peers:
            if peer.closed or peer.detached:
                continue
            if peer.suspect and peer.outstanding:
                continue   # Quiet in mid-transfer: its open requests are probe enough.
            room = (1 if peer.snubbed else peer.depth) - len(peer.outstanding)
            if room <= 0 and not waiting:
                continue
            for job in self.order:
                if job.done == job.count or job.abandoned:
                    continue
                if job.exclusive is not None and job.exclusive is not peer:
                    continue
                if peer.choked and not (peer.fast and job.index in peer.allowed_fast):
                    continue
                if not peer.has(job.index):
                    continue
                if job.urgent and (peer.suspect or peer.snubbed):
                    continue   # A peer that just went quiet gets no time-critical block.
                if job.cursor < job.count and not (job.urgent and self._defer(peer, job)):
                    block = job.state.find(0, job.cursor)
                    if job.urgent and block >= 0 and room <= 0:
                        # A block the reader waits for must not queue behind
                        # read-ahead: trade the far end of this pipeline for it.
                        room += self._preempt(peer, job.count - job.done - len(job.holders), now, outbox)
                    while block >= 0 and room > 0:
                        self._request(peer, job, block, now, outbox)
                        room -= 1
                        block = job.state.find(0, block + 1)
                    job.cursor = block if block >= 0 else job.count
                if room > 0 and job.exclusive is None and now >= job.hedge_after:
                    room = self._hedge(peer, job, room, now, outbox)
                if room <= 0 and not (waiting and job.urgent):
                    break

    @staticmethod
    def _quiet(peer, index):
        """This peer cannot be expected to deliver a block of this piece now."""
        if peer.closed or peer.snubbed or peer.suspect:
            return True
        # A choked BEP 6 peer still serves the pieces it declared allowed-fast.
        return peer.choked and not (peer.fast and index in peer.allowed_fast)

    def _reclaim(self, job):
        """Blocks of an awaited piece whose every holder went quiet count as unrequested.

        They then get a place near the front of a healthy pipeline like any
        other urgent block. The quiet peer's copy is still accepted if it comes.
        """
        for block, holders in list(job.holders.items()):
            if job.state[block] == 1 and all(self._quiet(holder, job.index) for holder in holders):
                for holder in list(holders):
                    self._release(holder, (job.index, block * BLOCK))

    def _preempt(self, peer, wanted, now, outbox):
        """Cancel read-ahead requests far back in this pipeline (cancel_non_critical)."""
        if wanted <= 0 or peer.snubbed:
            return 0
        far = [key for key, request in peer.outstanding.items()
               if request[2] > now + 0.5 and not getattr(self.jobs.get(key[0]), 'urgent', False)]
        far.sort(key=lambda key: -peer.outstanding[key][2])
        for key in far[:wanted]:
            self._cancel(peer, key, outbox)
        self.counters['cancelled_requests'] += min(wanted, len(far))
        return min(wanted, len(far))

    def _defer(self, peer, job):
        """A slow peer leaves an urgent piece alone when faster peers finish it sooner."""
        if job.exclusive is not None:
            return False   # Nobody else is allowed to deliver this piece.
        mine = peer.estimate()
        faster = 0.0
        for other in self.peers:
            # Only peers that are delivering right now can be relied on to finish it.
            if (other is not peer and other.rate > mine and not self._quiet(other, job.index)
                    and other.has(job.index)):
                faster += other.rate
        if not faster:
            return False
        remaining = (job.count - job.done) * BLOCK
        return remaining / faster < (peer.queued_bytes + BLOCK) / mine

    def _hedge(self, peer, job, room, now, outbox):
        """Let this peer rescue the block that is furthest from arriving.

        A block whose every holder went quiet is requested again, for any
        piece: otherwise silent peers end up holding the last blocks of every
        active piece and the healthy ones have nothing left to do. For a piece
        the reader waits for, an overdue block is also requested a second time,
        and a block that merely waits deep in a healthy pipeline is taken over
        (its old request is cancelled, so nothing is downloaded twice).
        """
        if peer.suspect or peer.snubbed:
            return room
        for _ in range(min(room, 4)):
            mine = (peer.queued_bytes + BLOCK) / peer.estimate()
            best, score, steal = None, 0.0, None
            for block, holders in job.holders.items():
                if peer in holders or job.state[block] == 2 or len(holders) >= 4:
                    continue
                healthy = [(holder, expected) for holder, expected in holders.items()
                           if not self._quiet(holder, job.index)]
                victim = None
                if not healthy:
                    value = 1000.0 + now - min(holders.values())
                elif not job.urgent or len(holders) > 1:
                    continue   # One healthy holder is enough outside urgent pieces.
                else:
                    holder, expected = healthy[0]
                    late = now - expected
                    if late > max(0.75, 2.0 * BLOCK / holder.estimate()) and mine <= max(0.5, late):
                        # Overdue, and this peer can deliver it sooner than the delay so
                        # far: a slightly late block is not worth a duplicate on a long queue.
                        value = 100.0 + late
                    elif expected - now > 2.0 * mine + 0.5:
                        value, victim = expected - now, holder
                    else:
                        continue
                if value > score:
                    best, score, steal = block, value, victim
            if best is None:
                job.hedge_after = now + 0.1
                break
            if steal is not None:
                self._cancel(steal, (job.index, best * BLOCK), outbox)
                self.counters['stolen_blocks'] += 1
            else:
                self.counters['hedged_blocks'] += 1
            self._request(peer, job, best, now, outbox)
            room -= 1
        return room

    def _snub(self, peer, now, outbox):
        """Stop trusting a silent peer: one probe request stays, the rest move on."""
        peer.snubbed, peer.snubbed_since, peer.quiet_since = True, now, now
        peer.depth = 1
        self.counters['snubs'] += 1
        keys = sorted(peer.outstanding, key=lambda key: peer.outstanding[key][1])
        for key in keys[1:]:
            self._cancel(peer, key, outbox)

    # ------------------------------------------------------------ periodic work
    def tick(self, now=None):
        now = clock() if now is None else now
        outbox = {}
        expired = []
        with self.lock:
            if now - self.last_tick < 0.2:
                return
            self.last_tick = now
            for peer in list(self.peers):
                if peer.closed:
                    continue
                if peer.outstanding:
                    idle = now - max(peer.last_block, peer.busy_since)
                    # A steady peer that suddenly misses many block intervals is
                    # set aside for urgent work long before the snub timeout.
                    if peer.rate and not peer.suspect and idle > max(1.0, 8.0 * BLOCK / peer.rate):
                        peer.suspect, peer.quiet_since = True, now
                        self.counters['suspected_stalls'] += 1
                    if not peer.snubbed and (not peer.choked or peer.fast) and idle > peer.timeout():
                        self._snub(peer, now, outbox)
                    elif peer.snubbed and now - peer.snubbed_since > 45:
                        expired.append(peer)
                elif (peer.snubbed or peer.suspect) and now - peer.quiet_since > SECOND_CHANCE:
                    # Its requests were all taken back, so no block can prove it alive
                    # again. Offer it a little work; a peer that wastes three such
                    # chances in a row is not coming back.
                    peer.chances += 1
                    if peer.chances > 3:
                        expired.append(peer)
                    else:
                        peer.snubbed = peer.suspect = False
                        peer.depth = 2
                if self.order and peer.known and not any(peer.has(job.index) for job in self.order):
                    peer.useless_since = peer.useless_since or now
                else:
                    peer.useless_since = 0.0
                if now - peer.last_sent > KEEPALIVE:
                    outbox.setdefault(peer, bytearray()).extend(b'\x00\x00\x00\x00')
            self._assign(self.peers, now, outbox)
        self._flush(outbox)
        for peer in expired:
            peer.close('Peer stopped sending requested data')

    # ------------------------------------------------------------ public API
    def promote(self, index):
        """A reader is now waiting for this piece: every peer should serve it first."""
        outbox = {}
        now = clock()
        with self.lock:
            job = self.jobs.get(index)
            if job is None:
                if len(self.hints) > 64:
                    self.hints.clear()
                self.hints.add(index)
                return
            if job.urgent:
                return
            self._make_urgent(job, now, outbox)
        self._flush(outbox)

    def _make_urgent(self, job, now, outbox):
        job.urgent = True
        self._reorder()
        # Assignment makes room for its unrequested blocks, fastest peers first.
        self._assign(self.peers, now, outbox)

    def demote(self, index):
        """The reader that waited for this piece was left behind by a seek."""
        with self.lock:
            self.hints.discard(index)
            job = self.jobs.get(index)
            if job is not None and job.urgent and job.pinned is None:
                job.urgent = False
                self._reorder()

    def set_focus(self, index):
        with self.lock:
            if index != self.focus:
                self.focus = index
                self._reorder()

    def abandon(self, index):
        """Drop a read-ahead piece nobody is waiting for (after a seek)."""
        outbox = {}
        with self.lock:
            self.hints.discard(index)
            job = self.jobs.get(index)
            if job is None or job.urgent:
                return False
            job.abandoned = True
            self._drop(job, outbox)
            job.finished.set()
        self._flush(outbox)
        return True

    def _drop(self, job, outbox):
        if job.done < job.count:
            self.counters['discarded_bytes'] += job.done * BLOCK   # Partial piece given up.
        for peer in list(self.peers):
            for key in [key for key in peer.outstanding if key[0] == job.index]:
                self._cancel(peer, key, outbox)

    def fetch(self, index, timeout=None, urgent=True, pinned=None):
        size = self.meta.piece_size(index)
        if timeout is None:
            timeout = min(90, max(45, 6 + size / (256 * 1024)))
        deadline = clock() + timeout
        outbox = {}
        now = clock()
        with self.lock:
            job = self.jobs.get(index)
            owner = job is None
            if owner:
                if index in self.hints:
                    self.hints.discard(index)
                    urgent = True
                job = self.jobs[index] = PieceJob(self.meta, index, urgent, pinned)
                self._reorder()
                if urgent:
                    self._make_urgent(job, now, outbox)
                else:
                    self._assign(self.peers, now, outbox)
            elif urgent and not job.urgent:
                self._make_urgent(job, now, outbox)
        self._flush(outbox)
        if not owner:
            while not job.settled.wait(0.2):
                if self.cancel.is_set() or clock() >= deadline:
                    raise TorrentError('Cancelled' if self.cancel.is_set() else 'Peers timed out')
            if job.error is not None:
                raise job.error
            return job.result
        try:
            job.result = self._download(job, deadline)
            return job.result
        except TorrentError as error:
            job.error = error
            raise
        finally:
            outbox = {}
            with self.lock:
                if self.jobs.get(index) is job:
                    del self.jobs[index]
                    self._reorder()
                self._drop(job, outbox)
            self._flush(outbox)
            job.settled.set()

    def _download(self, job, deadline):
        orphaned = None
        while True:
            while not job.finished.wait(0.2):
                now = clock()
                if self.cancel.is_set() or self.stopped:
                    raise TorrentError('Cancelled')
                peer = job.pinned
                if peer is not None:
                    if peer.closed:
                        raise TorrentError(peer.last_error or 'Peer closed or cancelled')
                    if peer.known and not peer.has(job.index):
                        raise MissingPiece('Peer does not have requested piece')
                if now >= deadline:
                    raise TorrentError(self._starved(job))
                self.tick(now)
                with self.lock:
                    chosen = job.exclusive
                    if peer is None and chosen is not None and (self._quiet(chosen, job.index)
                                                                or not chosen.has(job.index)):
                        # The peer chosen for the retry stopped serving: choose again,
                        # or let every peer help, rather than wait for the deadline.
                        job.exclusive = self._clean_peer(job)
                    alive = any(not other.closed for other in self.peers)
                if alive or self.pending():
                    orphaned = None
                elif orphaned is None:
                    orphaned = now   # A dropped peer is queued for redial a moment after it closes.
                elif now - orphaned >= 0.6:
                    raise TorrentError(self._starved(job))
            if self.cancel.is_set() or self.stopped:
                raise TorrentError('Cancelled')
            if job.abandoned:
                raise Abandoned('Abandoned read-ahead piece')
            if hashlib.sha1(job.buffer).digest() == self.meta.hashes[job.index]:
                self._settle(job)
                return job.buffer
            self._failed(job)

    def _settle(self, job):
        culprits = set()
        with self.lock:
            authors = set(job.authors)
            for peer in authors:
                if peer is not None:
                    peer.pieces += 1
            if job.suspects is not None:
                checksums, authors = job.suspects
                view = memoryview(job.buffer)
                for block, checksum in enumerate(checksums):
                    if zlib.crc32(view[block * BLOCK:block * BLOCK + job.length(block)]) != checksum:
                        culprits.add(authors[block])
                view.release()
        for peer in culprits:
            if peer is not None:
                self._ban(peer)

    def _failed(self, job):
        """A piece failed SHA-1: find out who sent the bad block before trusting anyone."""
        job.failures += 1
        self.counters['hash_failures'] += 1
        culprit = None
        with self.lock:
            if job.abandoned:
                # A seek dropped this piece while it was being hashed; resetting it
                # would clear the wake-up and leave its owner waiting for nothing.
                raise Abandoned('Abandoned read-ahead piece')
            authors = set(job.authors)
            if len(authors) == 1:
                culprit = next(iter(authors))
                keep_suspects = job.suspects
            else:
                view = memoryview(job.buffer)
                keep_suspects = ([zlib.crc32(view[block * BLOCK:block * BLOCK + job.length(block)])
                                  for block in range(job.count)], list(job.authors))
                view.release()
            outbox = {}
            self._drop(job, outbox)
            job.reset()
            job.suspects = keep_suspects
        self._flush(outbox)
        if culprit is not None:
            self._ban(culprit)
        if job.failures >= 4:
            raise TorrentError('Piece SHA-1 verification failed')
        outbox = {}
        with self.lock:
            if job.pinned is None and job.suspects is not None:
                # Download the retry from one peer, so the next result names the liar.
                job.exclusive = self._clean_peer(job)
            self._assign(self.peers, clock(), outbox)
        self._flush(outbox)

    def _clean_peer(self, job):
        suspects = set(job.suspects[1]) if job.suspects else set()
        candidates = [peer for peer in self.peers if not self._quiet(peer, job.index) and peer.has(job.index)]
        if not candidates:
            return None
        return max(candidates, key=lambda peer: (peer not in suspects, peer.estimate()))

    def _ban(self, peer):
        with self.lock:
            if peer.address not in self.banned:
                self.banned.add(peer.address)
                self.counters['banned_peers'] += 1
            # The same address may have been dialed again before the verdict.
            again = [other for other in self.peers if other.address == peer.address and other is not peer]
        for session in [peer] + again:
            session.close('Piece SHA-1 verification failed')

    def _starved(self, job):
        peer = job.pinned
        if peer is not None:
            return peer.last_error or 'Peer timed out waiting for an upload slot or requested blocks'
        if job.failures:
            return 'Piece SHA-1 verification failed'
        with self.lock:
            alive = [other for other in self.peers if not other.closed]
        if not alive:
            return 'No connected peer supplied a valid requested piece'
        owners = [other for other in alive if other.has(job.index)]
        if not owners:
            return 'No connected peer has this part of the video yet'
        if all(other.choked for other in owners):
            return 'Connected peers are busy and have not granted an upload slot'
        return 'Peers timed out while sending this part of the video'

    def stats(self):
        with self.lock:
            alive = [peer for peer in self.peers if not peer.closed]
            return {'connected_peers': len(alive),
                    'unchoked_peers': sum(not peer.choked for peer in alive),
                    'seeding_peers': sum(peer.complete for peer in alive),
                    'snubbed_peers': sum(peer.snubbed for peer in alive),
                    'fast_extension_peers': sum(peer.fast for peer in alive),
                    'peer_bytes_received': self.retired_bytes + sum(peer.bytes_received for peer in self.peers),
                    'swarm_rate': int(sum(peer.rate for peer in alive if not peer.choked and not peer.snubbed)),
                    'wasted_bytes': self.counters['wasted_bytes'],
                    'hedged_blocks': self.counters['hedged_blocks'],
                    'stolen_blocks': self.counters['stolen_blocks'],
                    'suspected_stalls': self.counters['suspected_stalls'],
                    'discarded_bytes': self.counters['discarded_bytes'],
                    'cancelled_requests': self.counters['cancelled_requests'],
                    'hash_failures': self.counters['hash_failures'],
                    'banned_peers': self.counters['banned_peers'],
                    'active_pieces': len(self.jobs)}

    def stop(self):
        with self.lock:
            self.stopped = True
            for job in self.jobs.values():
                job.finished.set()


class PeerPool:
    """Keeps enough useful connections alive and feeds them to the Swarm."""

    def __init__(self, meta, addresses, cancel=None, priority=(), max_connections=MAX_CONNECTIONS):
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
        self.max_connections = max(1, int(max_connections))
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        self.swarm = Swarm(meta, self.cancel, self.lock, pending=self._pending, notify=self._peer_changed)
        self.sessions = self.swarm.peers
        self.connecting = 0
        self.ready = threading.Event()
        self.connectors = []
        self.closed = False
        self.errors = 0
        self.error_details = Counter()
        self.refresher = None
        self.maintainer = None
        self.stopping = threading.Event()
        self.retries = []          # (due, address) for peers that delivered data and then dropped
        self.redials = Counter()
        self.pex = Counter()
        self.counters = Counter()
        self.last_eviction = 0.0

    @property
    def retired_bytes(self):
        return self.swarm.retired_bytes

    def _pending(self):
        return bool(self.connecting or self.queued or self.retries) and not self.closed

    def _live(self):
        return sum(not peer.closed for peer in self.sessions)

    def _next_address(self, now):
        self.retries = [entry for entry in self.retries if entry[1] not in self.swarm.banned]
        for position, (due, address) in enumerate(self.retries):
            if due <= now:
                del self.retries[position]
                return address
        while self.queued:
            address = self.queued.pop(0)
            if address not in self.swarm.banned:
                return address
        return None

    def _worker(self):
        while not self.closed and not self.cancel.is_set():
            with self.condition:
                address = None
                idle = clock()
                while not self.closed and not self.cancel.is_set():
                    if self._live() + self.connecting < self.max_connections:
                        address = self._next_address(clock())
                        if address is not None:
                            break
                    if clock() - idle > 10:
                        return   # Nothing to dial: new candidates start fresh workers.
                    self.condition.wait(0.5)
                if address is None:
                    return
                self.connecting += 1
            try:
                session = PeerSession(self.meta, address, self.cancel, self.swarm)
                with self.lock:
                    if self.closed:
                        session.close()
                        return
                    self.ready.set()
            except (OSError, TorrentError, ValueError) as error:
                with self.condition:
                    self.errors += 1
                    self.error_details[type(error).__name__ + ': ' + str(error)[:100]] += 1
            finally:
                with self.condition:
                    self.connecting -= 1
                    self.condition.notify_all()

    def _start_workers(self):
        with self.lock:
            # A dial costs a blocked thread, not memory; stale addresses time out
            # in parallel while max_connections bounds what stays connected.
            wanted = min(16, len(self.queued) + len(self.retries))
            self.connectors = [thread for thread in self.connectors if thread.is_alive()]
            alive = len(self.connectors)
            for _ in range(max(0, wanted - alive)):
                thread = threading.Thread(target=self._worker, daemon=True)
                self.connectors.append(thread)
                thread.start()
            if self.maintainer is None and not self.closed:
                self.maintainer = threading.Thread(target=self._maintain, daemon=True)
                self.maintainer.start()

    def prepare(self, timeout=15):
        self._start_workers()
        deadline = time.monotonic() + timeout
        while not self.ready.wait(0.1):
            # Every candidate refused or timed out: fail now so another source can start.
            if (self.cancel.is_set() or time.monotonic() >= deadline
                    or (not self._pending() and not self._live())):
                raise TorrentError('No usable TCP peers connected')

    def fetch(self, index, timeout=None, urgent=True):
        try:
            return self.swarm.fetch(index, timeout=timeout, urgent=urgent)
        except TorrentError as error:
            if not isinstance(error, Abandoned) and 'Cancelled' not in str(error):
                with self.condition:
                    self.errors += 1
                    self.error_details[type(error).__name__ + ': ' + str(error)[:100]] += 1
            raise

    def unchoked(self):
        with self.lock:
            return sum(not peer.closed and not peer.choked for peer in self.sessions)

    def promote(self, index):
        self.swarm.promote(index)

    def demote(self, index):
        self.swarm.demote(index)

    def set_focus(self, index):
        self.swarm.set_focus(index)

    def abandon(self, index):
        return self.swarm.abandon(index)

    def _peer_changed(self, peer):
        with self.condition:
            found = peer.new_peers
            if found:
                peer.new_peers = []
                self._enqueue(found)
            if peer.closed and not getattr(peer, 'accounted', False):
                peer.accounted = True
                self.pex.update(peer.pex_flags)
                if peer.last_error and not self.closed:
                    self.error_details['Peer closed: ' + peer.last_error[:80]] += 1
                # A peer that was delivering data probably dropped with the network;
                # dial it again instead of forgetting a proven source.
                if (not self.closed and peer.blocks and peer.address not in self.swarm.banned
                        and not getattr(peer, 'evicted', False) and self.redials[peer.address] < 2):
                    self.redials[peer.address] += 1
                    self.counters['redials'] += 1
                    self.retries.append((clock() + 2 + 6 * (self.redials[peer.address] - 1), peer.address))
            self.condition.notify_all()

    def _enqueue(self, addresses):
        if self.closed:
            return
        for address in addresses:
            if address not in self.seen and len(self.seen) < 600:
                self.seen.add(address)
                self.queued.append(address)

    def add_peers(self, addresses):
        with self.condition:
            self._enqueue(addresses)
            self.condition.notify_all()
        if not self.closed and self.connectors:
            self._start_workers()

    def refresh(self, discover):
        def work():
            delay = 1
            while not self.stopping.wait(delay):
                if self.closed or self.cancel.is_set():
                    return
                try:
                    self.add_peers(discover())
                except Exception:
                    pass   # Tracker and DHT replies are untrusted; the refresh must survive them.
                # Ask again soon while the swarm is thin; a healthy one needs no refresh.
                delay = 60 if self._live() >= 6 else 20
        self.refresher = threading.Thread(target=work, daemon=True)
        self.refresher.start()

    def _maintain(self):
        while not self.stopping.wait(0.5):
            if self.closed or self.cancel.is_set():
                return
            try:
                self.swarm.tick()
                self._evict()
            except (OSError, TorrentError, ValueError):
                pass
            with self.condition:
                waiting = bool(self.queued or self.retries) and self._live() + self.connecting < self.max_connections
                if self.retries:
                    self.condition.notify_all()
            if waiting:
                self._start_workers()

    def _evict(self):
        """Give a waiting candidate the slot of the least useful connection."""
        now = clock()
        victim = None
        with self.lock:
            if (not self.queued or now - self.last_eviction < 2
                    or self._live() + self.connecting < self.max_connections):
                return
            worst = 0.0
            for peer in self.sessions:
                if peer.closed or peer.outstanding and not peer.snubbed:
                    continue
                score = 0.0
                if peer.choked and now - max(peer.choked_since, peer.last_block) > CHOKED_EVICTION:
                    score = 3.0 + (now - peer.choked_since) / 1000.0
                elif peer.snubbed and now - peer.snubbed_since > SNUB_EVICTION:
                    score = 2.0
                elif peer.useless_since and now - peer.useless_since > USELESS_EVICTION:
                    score = 1.0 + (now - peer.useless_since) / 1000.0
                if score > worst:
                    victim, worst = peer, score
            if victim is None:
                return
            self.last_eviction = now
            self.counters['evicted_peers'] += 1
            victim.evicted = True
        victim.close('Replaced by a new candidate')

    def stats(self):
        result = self.swarm.stats()
        with self.lock:
            result.update({'queued_peers': len(self.queued), 'peer_errors': self.errors,
                           'peer_error_details': dict(self.error_details.most_common(6)),
                           'connecting_peers': self.connecting, 'known_peers': len(self.seen),
                           'evicted_peers': self.counters['evicted_peers'], 'redials': self.counters['redials'],
                           'max_connections': self.max_connections})
            pex = Counter(self.pex)
            for peer in self.sessions:
                pex.update(peer.pex_flags)
            if pex['announced']:
                result['pex_encryption_preferred_percent'] = round(100 * pex['encryption_preferred'] / pex['announced'])
        return result

    def close(self):
        self.closed = True
        self.stopping.set()
        self.swarm.stop()
        with self.condition:
            sessions = list(self.sessions)
            self.condition.notify_all()
        for peer in sessions:
            peer.close()
        deadline = time.monotonic() + 10
        threads = self.connectors + [thread for thread in (self.refresher, self.maintainer) if thread]
        for thread in threads:
            if thread is not threading.current_thread():
                thread.join(timeout=max(0, deadline - time.monotonic()))
        for peer in sessions:
            peer.join(max(0, min(1, deadline - time.monotonic())))
