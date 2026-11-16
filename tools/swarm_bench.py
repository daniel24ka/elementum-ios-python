"""Controlled streaming benchmark: a scripted loopback swarm and a Kodi-like reader.

The swarm runs in its own process so that its threads never share the engine's
GIL and never count toward the engine's memory. Every peer has an explicit
transfer rate, latency and fault script, so two engine versions can be compared
on the same conditions:

    python tools/swarm_bench.py run --lib addon/resources/lib --out new.json
    python tools/swarm_bench.py run --lib ../baseline/addon/resources/lib --out old.json
    python tools/swarm_bench.py compare old.json new.json

Rates are real (wall-clock) rates on real TCP sockets; nothing is mocked inside
the engine except magnet resolution, which is replaced by the scripted swarm.
"""

import argparse
import hashlib
import http.client
import json
import os
import socket
import socketserver
import struct
import subprocess
import sys
import tempfile
import threading
import time
from collections import OrderedDict
from urllib.parse import urlsplit

KIB, MIB = 1024, 1024 * 1024
BLOCK = 16384


# ---------------------------------------------------------------- scenarios
def peers(count, rate, **options):
    return [dict(rate=rate, **options) for _ in range(count)]


SCENARIOS = OrderedDict((
    ('healthy', dict(
        note='10 peers x 400 KiB/s, 256 KiB pieces; the easy case must not regress',
        size=48 * MIB, piece=256 * KIB, bitrate=MIB, play=24, seek_at=8, seek_to=.6,
        peers=peers(10, 400 * KIB))),
    ('many_slow', dict(
        note='16 peers x 150 KiB/s; enough capacity only if all of them are used',
        size=48 * MIB, piece=MIB, bitrate=MIB, play=24, seek_at=8, seek_to=.6,
        peers=peers(16, 150 * KIB))),
    ('mixed', dict(
        note='2 fast peers and 8 very slow ones; a slow peer must not hold an urgent piece',
        size=48 * MIB, piece=2 * MIB, bitrate=MIB, play=24, seek_at=8, seek_to=.6,
        peers=peers(2, 1500 * KIB) + peers(8, 80 * KIB))),
    ('stalling', dict(
        note='6 peers x 500 KiB/s; two of them go silent mid-transfer but stay connected',
        size=48 * MIB, piece=MIB, bitrate=MIB, play=24, seek_at=8, seek_to=.6,
        peers=peers(4, 500 * KIB) + peers(2, 500 * KIB, stall_after=3 * MIB))),
    ('large_pieces', dict(
        note='8 MiB pieces at 2.5 MiB/s video; one piece from one peer is slower than playback',
        size=120 * MIB, piece=8 * MIB, bitrate=2560 * KIB, play=24, seek_at=8, seek_to=.6,
        peers=peers(8, 600 * KIB))),
    ('choked_majority', dict(
        note='4 useful peers hidden among 10 that never unchoke and 30 dead addresses',
        size=48 * MIB, piece=MIB, bitrate=MIB, play=24, seek_at=8, seek_to=.6, dead=30,
        peers=peers(10, 500 * KIB, never_unchoke=True) + peers(4, 500 * KIB))),
    ('partial', dict(
        note='half of the swarm only has the first 30% of the file',
        size=48 * MIB, piece=MIB, bitrate=MIB, play=24, seek_at=8, seek_to=.6,
        peers=peers(6, 400 * KIB, have=.3) + peers(6, 300 * KIB))),
    ('churn', dict(
        note='every peer disconnects after 6-14 seconds; spare peers must be picked up',
        size=48 * MIB, piece=MIB, bitrate=MIB, play=24, seek_at=8, seek_to=.6,
        peers=[dict(rate=400 * KIB, drop_after=6 + (index % 5) * 2) for index in range(10)]
              + peers(14, 400 * KIB, unchoke_delay=1))),
))


def matroska_header(seconds):
    """The smallest Matroska start that states a duration, like a real film does."""
    info = b'\x2a\xd7\xb1\x83\x0f\x42\x40' + b'\x44\x89\x88' + struct.pack('>d', seconds * 1000.0)
    return (b'\x1a\x45\xdf\xa3\x84\x42\x86\x81\x01' + b'\x18\x53\x80\x67\x01' + b'\xff' * 7 +
            b'\x15\x49\xa9\x66' + bytes([0x80 | len(info)]) + info)


def piece_bytes(seed, index, size, seconds=0):
    data = hashlib.shake_128(seed + struct.pack('!I', index)).digest(size)
    if index == 0 and seconds:
        header = matroska_header(seconds)
        data = header + data[len(header):]
    return data


def build_metainfo(scenario, seed):
    size, piece = scenario['size'], scenario['piece']
    count = (size + piece - 1) // piece
    seconds = size / scenario['bitrate']
    hashes = b''.join(hashlib.sha1(piece_bytes(seed, index, min(piece, size - index * piece), seconds)).digest()
                      for index in range(count))
    return {b'info': {b'name': b'bench.mkv', b'length': size, b'piece length': piece, b'pieces': hashes}}


# ---------------------------------------------------------------- the swarm
class Swarm:
    def __init__(self, scenario, seed):
        self.scenario, self.seed = scenario, seed
        self.size, self.piece = scenario['size'], scenario['piece']
        self.count = (self.size + self.piece - 1) // self.piece
        self.lock = threading.Lock()
        self.cache = OrderedDict()
        self.sent = []            # (time, peer, piece index, bytes)
        self.connections = 0
        self.keepalives = 0
        self.started = time.monotonic()
        self.servers = []
        self.info_hash = None

    def content(self, index):
        with self.lock:
            data = self.cache.get(index)
            if data is not None:
                self.cache.move_to_end(index)
                return data
        data = piece_bytes(self.seed, index, min(self.piece, self.size - index * self.piece),
                           self.size / self.scenario['bitrate'])
        with self.lock:
            self.cache[index] = data
            while len(self.cache) > 48:
                self.cache.popitem(last=False)
        return data

    def serve(self, info_hash):
        self.info_hash = info_hash
        addresses = []
        for number, profile in enumerate(self.scenario['peers']):
            server = _PeerServer(('127.0.0.1', 0), _PeerHandler)
            server.swarm, server.profile, server.number = self, profile, number
            threading.Thread(target=server.serve_forever, daemon=True).start()
            self.servers.append(server)
            addresses.append(list(server.server_address))
        for _ in range(self.scenario.get('dead', 0)):
            # A bound but never-accepting socket: connections are refused or hang
            # exactly like a stale tracker entry.
            placeholder = socket.socket()
            placeholder.bind(('127.0.0.1', 0))
            addresses.append(list(placeholder.getsockname()))
            placeholder.close()
        return addresses

    def close(self):
        for server in self.servers:
            server.shutdown()
            server.server_close()

    def stats(self):
        with self.lock:
            sent = list(self.sent)
        return {'connections': self.connections, 'wire_bytes': sum(item[3] for item in sent),
                'blocks': [[round(item[0] - self.started, 3), item[1], item[2], item[3]] for item in sent]}


class _PeerServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 64


def _recv(sock, count):
    data = bytearray()
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk:
            raise OSError('closed')
        data.extend(chunk)
    return bytes(data)


class _PeerHandler(socketserver.BaseRequestHandler):
    def handle(self):
        swarm, profile, number = self.server.swarm, self.server.profile, self.server.number
        sock = self.request
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        send_lock = threading.Lock()
        state = {'queue': [], 'choking': True, 'closed': False, 'sent': 0, 'choked_once': False, 'fast': False}
        condition = threading.Condition()
        with swarm.lock:
            swarm.connections += 1

        def send(payload):
            with send_lock:
                sock.sendall(struct.pack('!I', len(payload)) + payload)

        have = profile.get('have', 1)
        owned = int(swarm.count * have) if have < 1 else swarm.count

        def sender():
            opened = time.monotonic()
            next_slot = opened
            unchoke_at = opened + profile.get('unchoke_delay', 0)
            try:
                while not state['closed']:
                    now = time.monotonic()
                    if profile.get('drop_after') and now - opened >= profile['drop_after']:
                        raise OSError('scripted disconnect')
                    choke_at = profile.get('choke_at')
                    if choke_at is not None and not state['choked_once'] and now - opened >= choke_at:
                        state['choked_once'] = True
                        with condition:
                            dropped, state['queue'] = state['queue'], []
                            state['choking'] = True
                        unchoke_at = now + profile.get('choke_for', 1)
                        send(b'\x00')
                        if state['fast']:
                            for _, index, begin, length in dropped:
                                send(struct.pack('!BIII', 16, index, begin, length))
                        continue
                    with condition:
                        if state['choking'] and not profile.get('never_unchoke') and now >= unchoke_at:
                            state['choking'] = False
                            unchoke = True
                        else:
                            unchoke = False
                        stalled = profile.get('stall_after') and state['sent'] >= profile['stall_after']
                        request = state['queue'][0] if state['queue'] and not state['choking'] and not stalled else None
                        if request is None and not unchoke:
                            condition.wait(.05)
                            continue
                    if unchoke:
                        send(b'\x01')
                        continue
                    arrival, index, begin, length = request
                    ready = max(next_slot, arrival + profile.get('latency', 0))
                    if ready > now:
                        time.sleep(min(.05, ready - now))
                        continue
                    with condition:
                        if not state['queue'] or state['queue'][0] is not request:
                            continue  # Cancelled while pacing.
                        state['queue'].pop(0)
                    data = swarm.content(index)[begin:begin + length]
                    if profile.get('corrupt'):
                        data = bytes([data[0] ^ 1]) + data[1:]
                    send(struct.pack('!BII', 7, index, begin) + data)
                    state['sent'] += length
                    next_slot = max(next_slot, now - .05) + length / profile['rate']
                    with swarm.lock:
                        swarm.sent.append((time.monotonic(), number, index, length))
            except OSError:
                pass
            finally:
                state['closed'] = True
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

        try:
            sock.settimeout(120)
            greeting = _recv(sock, 68)
            if greeting[28:48] != swarm.info_hash:
                return
            reserved = bytearray(8)
            reserved[5] = 0x10
            fast = bool(profile.get('fast') and greeting[27] & 0x04)
            state['fast'] = fast
            if fast:
                reserved[7] |= 0x04
            sock.sendall(greeting[:20] + bytes(reserved) + swarm.info_hash + b'-SIM000-' + os.urandom(12))
            bits = bytearray((swarm.count + 7) // 8)
            for index in range(owned):
                bits[index // 8] |= 128 >> (index % 8)
            send(b'\x0e' if fast and owned == swarm.count else b'\x05' + bytes(bits))
            rejected = 0
            send(b'\x14\x00' + b'd1:md6:ut_pexi1ee4:reqqi250e1:v7:SimPeere')
            threading.Thread(target=sender, daemon=True).start()
            while not state['closed']:
                length = struct.unpack('!I', _recv(sock, 4))[0]
                if not length:
                    with swarm.lock:
                        swarm.keepalives += 1
                    continue
                message = _recv(sock, length)
                kind = message[0]
                if kind == 6 and len(message) == 13:
                    index, begin, size = struct.unpack('!III', message[1:])
                    if index >= owned or size > BLOCK or begin + size > swarm.piece:
                        raise OSError('invalid request')
                    if fast and (state['choking'] or rejected < profile.get('reject_first', 0)):
                        rejected += 1
                        send(b'\x10' + message[1:])   # BEP 6: every request gets an answer.
                        continue
                    with condition:
                        if not state['choking'] and len(state['queue']) < 250:
                            state['queue'].append((time.monotonic(), index, begin, size))
                            condition.notify()
                elif kind == 8 and len(message) == 13:
                    index, begin, size = struct.unpack('!III', message[1:])
                    with condition:
                        state['queue'] = [item for item in state['queue'] if item[1:] != (index, begin, size)]
        except (OSError, struct.error) as error:
            if os.environ.get('BENCH_DEBUG'):
                print('peer {} connection ended: {!r} after {} bytes'.format(number, error, state['sent']),
                      file=sys.stderr, flush=True)
        finally:
            state['closed'] = True
            with condition:
                condition.notify_all()


def swarm_main(arguments):
    scenario = SCENARIOS[arguments.scenario]
    seed = arguments.seed.encode('ascii')
    swarm = Swarm(scenario, seed)
    info_hash = bytes.fromhex(arguments.info_hash)
    addresses = swarm.serve(info_hash)
    print(json.dumps({'peers': addresses}), flush=True)
    for line in sys.stdin:
        if line.strip() == 'stats':
            print(json.dumps(swarm.stats()), flush=True)
        elif line.strip() == 'quit':
            break


# ---------------------------------------------------------------- the player
def peak_memory():
    try:
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [('cb', wintypes.DWORD), ('PageFaultCount', wintypes.DWORD)] + [
                (name, ctypes.c_size_t) for name in (
                    'PeakWorkingSetSize', 'WorkingSetSize', 'QuotaPeakPagedPoolUsage', 'QuotaPagedPoolUsage',
                    'QuotaPeakNonPagedPoolUsage', 'QuotaNonPagedPoolUsage', 'PagefileUsage', 'PeakPagefileUsage')]
        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        process = ctypes.windll.kernel32.GetCurrentProcess
        process.restype = wintypes.HANDLE
        info = ctypes.windll.psapi.GetProcessMemoryInfo
        info.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
        if info(process(), ctypes.byref(counters), counters.cb):
            return counters.PeakWorkingSetSize
    except (ImportError, AttributeError, OSError):
        pass
    try:
        import resource
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return peak if sys.platform == 'darwin' else peak * 1024
    except ImportError:
        return 0


class Player:
    """Reads like Kodi: head probe, tail probe, then one long range per position.

    Playback starts once START seconds are buffered, consumes bytes at the video
    bitrate, and stalls when the buffer empties. The forward cache is bounded
    like Kodi's memory cache, so the reader does not pull the whole file.
    """
    START, RESUME, CACHE = 1.5, 2.0, 48 * MIB

    def __init__(self, url, size, bitrate):
        self.url, self.size, self.bitrate = urlsplit(url), size, bitrate
        self.lock = threading.Lock()
        self.received = 0          # Absolute end offset of contiguous received bytes.
        self.position = 0.0        # Absolute playback byte position.
        self.generation = 0
        self.errors = []
        self.delivered = 0
        self.first_byte = None

    def fetch(self, start, limit=None):
        connection = http.client.HTTPConnection(self.url.hostname, self.url.port, timeout=60)
        connection.request('GET', self.url.path, headers={'Range': 'bytes={}-'.format(start)})
        response = connection.getresponse()
        if response.status not in (200, 206):
            connection.close()
            raise OSError('HTTP {}'.format(response.status))
        return connection, response

    def probe(self, start, length):
        connection, response = self.fetch(start)
        try:
            remaining = min(length, self.size - start)
            while remaining:
                chunk = response.read(min(65536, remaining))
                if not chunk:
                    raise OSError('short probe')
                if self.first_byte is None:
                    self.first_byte = time.monotonic()
                remaining -= len(chunk)
        finally:
            connection.close()

    def reader(self, start, generation):
        try:
            connection, response = self.fetch(start)
        except OSError as error:
            self.errors.append(str(error))
            return
        try:
            while True:
                with self.lock:
                    if generation != self.generation:
                        return
                    ahead = self.received - self.position
                if ahead >= self.CACHE:
                    time.sleep(.02)
                    continue
                chunk = response.read1(65536)
                if not chunk:
                    return
                with self.lock:
                    if generation != self.generation:
                        return
                    self.received += len(chunk)
                    self.delivered += len(chunk)
        except (OSError, http.client.HTTPException) as error:
            with self.lock:
                if generation == self.generation:
                    self.errors.append(type(error).__name__ + ': ' + str(error)[:80])
        finally:
            connection.close()

    def open(self, offset):
        with self.lock:
            self.generation += 1
            generation = self.generation
            self.received = offset
            self.position = float(offset)
        threading.Thread(target=self.reader, args=(offset, generation), daemon=True).start()

    def wait_buffer(self, seconds, timeout):
        deadline = time.monotonic() + timeout
        need = min(seconds * self.bitrate, self.size - self.position)
        while time.monotonic() < deadline:
            with self.lock:
                if self.received - self.position >= need - 1:
                    return True
                if self.errors:
                    return False
            time.sleep(.01)
        return False

    def play(self, seconds, on_tick):
        """Advance the playback clock; returns the list of stall durations."""
        stalls, played, last = [], 0.0, time.monotonic()
        stall_started = None
        while played < seconds:
            time.sleep(.02)
            now = time.monotonic()
            step, last = now - last, now
            with self.lock:
                ahead = self.received - self.position
                complete = self.received >= self.size
                finished = self.position >= self.size - self.bitrate * .1
                failed = bool(self.errors)
            if finished:
                break
            if stall_started is not None:
                if ahead >= min(self.RESUME * self.bitrate, self.size - self.position) - 1:
                    stalls.append(now - stall_started)
                    stall_started = None
                elif failed or now - stall_started > 90:
                    stalls.append(now - stall_started)
                    return stalls, played, 'aborted'
                on_tick(True)
                continue
            if ahead < self.bitrate * .05 and not complete:
                stall_started = now
                on_tick(True)
                continue
            advance = min(step * self.bitrate, ahead)
            with self.lock:
                self.position += advance
            played += advance / self.bitrate
            on_tick(False)
        return stalls, played, 'ok'


def run_scenario(name, lib, seed='bench', verbose=False):
    scenario = SCENARIOS[name]
    sys.path.insert(0, os.path.abspath(lib))
    from elementum_py import engine as engine_module
    from elementum_py.core import Metainfo, bencode

    def encode(value):
        return bencode(value)

    envelope = build_metainfo(scenario, seed.encode('ascii'))
    meta = Metainfo(encode(envelope))
    swarm = subprocess.Popen([sys.executable, os.path.abspath(__file__), 'swarm', '--scenario', name,
                              '--seed', seed, '--info-hash', meta.info_hash.hex()],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    result = {'scenario': name, 'note': scenario['note']}
    engine = None
    try:
        addresses = [tuple(item) for item in json.loads(swarm.stdout.readline())['peers']]
        import random
        random.Random(7).shuffle(addresses)
        engine_module.resolve_magnet = lambda uri, **kwargs: (meta, list(addresses))
        settings = {'download_storage': '1', 'dht_enabled': 'false', 'stream_cache_mb': '32'}
        profile = tempfile.mkdtemp(prefix='elementum-bench-')
        engine = engine_module.TorrentEngine(profile, lambda key: settings.get(key, ''))
        size, bitrate = scenario['size'], scenario['bitrate']
        duration = size / bitrate
        started = time.monotonic()
        try:
            prepared = engine.prepare('magnet:?xt=urn:btih:' + meta.info_hash.hex(), peer_only=True)
        except Exception as error:  # The benchmark must report, never hide, a failed start.
            result.update(status='prepare_failed', error=str(error)[:200],
                          prepare_seconds=round(time.monotonic() - started, 2))
            return result
        result['prepare_seconds'] = round(time.monotonic() - started, 2)
        store = engine.active['store']
        player = Player(prepared['url'], size, bitrate)
        peak = {'cached': 0, 'peers': 0, 'unchoked': 0}

        trace = {'next': 0.0}

        def tick(caching):
            now = time.monotonic()
            if now < trace['next']:
                return
            trace['next'] = now + .5  # The Kodi service loop samples twice a second.
            try:
                store.update_playback(duration, caching)
                stats = engine.stats()
                peak['cached'] = max(peak['cached'], stats.get('cached_bytes', 0))
                peak['peers'] = max(peak['peers'], stats.get('connected_peers', 0))
                peak['unchoked'] = max(peak['unchoked'], stats.get('unchoked_peers', 0))
                if os.environ.get('BENCH_TRACE'):
                    print('{:6.1f}s {} pos={:.2f} ahead={:.2f}MiB verified={:.1f}MiB peers={}/{}'.format(
                        now - started, 'STALL' if caching else 'play ', player.position / MIB,
                        (player.received - player.position) / MIB, store.network_bytes / MIB,
                        stats.get('unchoked_peers'), stats.get('connected_peers')), file=sys.stderr, flush=True)
            except Exception:
                pass

        try:
            player.probe(0, 512 * KIB)
            player.probe(max(0, size - 256 * KIB), 256 * KIB)
        except OSError as error:
            result.update(status='probe_failed', error=str(error)[:200])
            return result
        result['first_byte_seconds'] = round(player.first_byte - started, 2)
        player.open(0)
        if not player.wait_buffer(Player.START, 90):
            result.update(status='start_failed', error='; '.join(player.errors)[:200])
            return result
        result['startup_seconds'] = round(time.monotonic() - started, 2)
        stalls, played, state = player.play(scenario['seek_at'], tick)
        all_stalls = list(stalls)
        seek_time = None
        if state == 'ok':
            target = int(size * scenario['seek_to']) // BLOCK * BLOCK
            seek_started = time.monotonic()
            seek_clock = seek_started - started
            player.open(target)
            if player.wait_buffer(Player.START, 90):
                seek_time = time.monotonic() - seek_started
                more, played_more, state = player.play(scenario['play'] - scenario['seek_at'], tick)
                all_stalls += more
                played += played_more
            else:
                state = 'seek_failed'
            result['seek_seconds'] = round(seek_time, 2) if seek_time is not None else None
            result['seek_clock'] = round(seek_clock, 2)
        elapsed = time.monotonic() - started
        stats = engine.stats()
        result.update(status=state, played_seconds=round(played, 2), wall_seconds=round(elapsed, 2),
                      rebuffer_count=len(all_stalls), rebuffer_seconds=round(sum(all_stalls), 2),
                      longest_rebuffer=round(max(all_stalls), 2) if all_stalls else 0,
                      delivered_mib=round(player.delivered / MIB, 2),
                      verified_mib=round(store.network_bytes / MIB, 2),
                      peak_cached_mib=round(peak['cached'] / MIB, 2), peak_connected=peak['peers'],
                      peak_unchoked=peak['unchoked'], player_errors=player.errors[:3],
                      http=stats.get('http', {}))
        if 'wasted_bytes' in stats:
            # Only the block-level engine counts these itself.
            result['duplicate_mib'] = round(stats['wasted_bytes'] / MIB, 2)
            result['discarded_partial_mib'] = round(stats.get('discarded_bytes', 0) / MIB, 2)
        if verbose:
            result['engine_stats'] = {key: value for key, value in stats.items() if key != 'peer_error_details'}
        swarm.stdin.write('stats\n')
        swarm.stdin.flush()
        wire = json.loads(swarm.stdout.readline())
        result['wire_mib'] = round(wire['wire_bytes'] / MIB, 2)
        result['swarm_connections'] = wire['connections']
        if store.network_bytes:
            result['wire_overhead_percent'] = round(100 * (wire['wire_bytes'] / store.network_bytes - 1), 1)
        if result.get('seek_clock') is not None:
            # Bytes still sent for the abandoned position after the seek settled.
            piece = scenario['piece']
            boundary = int(size * scenario['seek_to']) // piece
            late = sum(block[3] for block in wire['blocks']
                       if block[0] > result['seek_clock'] + 1 and 0 < block[2] < boundary - 1)
            result['obsolete_after_seek_mib'] = round(late / MIB, 2)
        result['peak_rss_mib'] = round(peak_memory() / MIB, 1)
        return result
    finally:
        if engine:
            try:
                engine.stop()
            except Exception:
                pass
        try:
            swarm.stdin.write('quit\n')
            swarm.stdin.flush()
        except (OSError, ValueError):
            pass
        swarm.kill()


def run_main(arguments):
    names = arguments.scenario or list(SCENARIOS)
    results = []
    for name in names:
        for repeat in range(arguments.repeat):
            # One fresh interpreter per run keeps peak memory and threads comparable.
            output = subprocess.run([sys.executable, os.path.abspath(__file__), 'one', '--lib', arguments.lib,
                                     '--scenario', name] + (['--verbose'] if arguments.verbose else []),
                                    capture_output=True, text=True, timeout=600)
            try:
                result = json.loads(output.stdout.strip().splitlines()[-1])
            except (ValueError, IndexError):
                result = {'scenario': name, 'status': 'crashed', 'error': output.stderr[-400:]}
            result['repeat'] = repeat
            results.append(result)
            print(json.dumps(result), flush=True)
    if arguments.out:
        with open(arguments.out, 'w', encoding='utf-8') as handle:
            json.dump({'lib': os.path.abspath(arguments.lib), 'python': sys.version.split()[0],
                       'results': results}, handle, indent=1)


def median(values):
    values = sorted(value for value in values if value is not None)
    if not values:
        return None
    middle = len(values) // 2
    return values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2


def compare_main(arguments):
    tables = []
    for path in arguments.files:
        with open(path, encoding='utf-8') as handle:
            tables.append(json.load(handle)['results'])
    fields = ('startup_seconds', 'seek_seconds', 'rebuffer_count', 'rebuffer_seconds', 'longest_rebuffer',
              'obsolete_after_seek_mib', 'wire_overhead_percent', 'duplicate_mib', 'peak_connected', 'peak_rss_mib')
    for name in SCENARIOS:
        print('\n' + name + ' - ' + SCENARIOS[name]['note'])
        for field in fields:
            row = []
            for table in tables:
                runs = [item for item in table if item['scenario'] == name]
                values = [item.get(field) for item in runs]
                failed = sum(item.get('status') != 'ok' for item in runs)
                value = median(values)
                row.append(('-' if value is None else '{:g}'.format(round(value, 2))) +
                           (' ({} of {} runs failed)'.format(failed, len(runs)) if failed and field == fields[0] else ''))
            print('  {:26}{}'.format(field, '   ->   '.join(row)))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest='command', required=True)
    swarm = commands.add_parser('swarm')
    swarm.add_argument('--scenario', required=True)
    swarm.add_argument('--seed', default='bench')
    swarm.add_argument('--info-hash', required=True)
    for name in ('run', 'one'):
        command = commands.add_parser(name)
        command.add_argument('--lib', required=True, help='directory that contains the elementum_py package')
        command.add_argument('--scenario', action='append' if name == 'run' else 'store')
        command.add_argument('--verbose', action='store_true')
        if name == 'run':
            command.add_argument('--repeat', type=int, default=1)
            command.add_argument('--out')
    compare = commands.add_parser('compare')
    compare.add_argument('files', nargs='+')
    arguments = parser.parse_args()
    if arguments.command == 'swarm':
        swarm_main(arguments)
    elif arguments.command == 'one':
        print(json.dumps(run_scenario(arguments.scenario, arguments.lib, verbose=arguments.verbose)), flush=True)
    elif arguments.command == 'run':
        run_main(arguments)
    else:
        compare_main(arguments)


if __name__ == '__main__':
    main()
