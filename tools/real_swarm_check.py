"""One real peer-to-peer session against a public swarm, without Kodi.

    python tools/real_swarm_check.py --lib addon/resources/lib

Default: Sintel, the Blender Foundation's Creative Commons film, peers only
(webseeds disabled). It resolves the magnet, prepares the stream, reads from the
start for a while and then seeks, exactly through the local HTTP interface.

Public swarms change from minute to minute. This proves that the engine talks
to real clients; it is not a benchmark and must not be quoted as one.
Use tools/swarm_bench.py for comparisons.
"""

import argparse
import http.client
import json
import os
import sys
import tempfile
import time
from urllib.parse import urlsplit

SINTEL = ('magnet:?xt=urn:btih:08ada5a7a6183aae1e09d831df6748d566095a10&dn=Sintel'
          '&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fexplodie.org%3A6969')
MIB = 1024 * 1024


def read_for(url, start, seconds, limit):
    parsed = urlsplit(url)
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=120)
    started = time.monotonic()
    connection.request('GET', parsed.path, headers={'Range': 'bytes={}-'.format(start)})
    response = connection.getresponse()
    first, received = None, 0
    try:
        if response.status not in (200, 206):
            return {'status': response.status}
        while time.monotonic() - started < seconds and received < limit:
            chunk = response.read1(65536)
            if not chunk:
                break
            if first is None:
                first = time.monotonic() - started
            received += len(chunk)
    finally:
        connection.close()
    elapsed = time.monotonic() - started
    return {'status': response.status, 'first_byte_seconds': round(first, 2) if first is not None else None,
            'mib': round(received / MIB, 2), 'seconds': round(elapsed, 2),
            'mib_per_second': round(received / MIB / max(elapsed, .001), 2)}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--lib', required=True, help='directory that contains the elementum_py package')
    parser.add_argument('--magnet', default=SINTEL)
    parser.add_argument('--seconds', type=float, default=20)
    parser.add_argument('--limit-mib', type=float, default=60)
    arguments = parser.parse_args()
    sys.path.insert(0, os.path.abspath(arguments.lib))
    from elementum_py.engine import TorrentEngine

    settings = {'download_storage': '1', 'stream_cache_mb': '32'}
    stages = []
    engine = TorrentEngine(tempfile.mkdtemp(prefix='elementum-real-'), lambda key: settings.get(key, ''),
                           lambda *message: stages.append((round(time.monotonic() - started, 1), message[0])))
    started = time.monotonic()
    report = {'lib': os.path.abspath(arguments.lib)}
    try:
        prepared = engine.prepare(arguments.magnet, peer_only=True)
        report['prepare_seconds'] = round(time.monotonic() - started, 2)
        report['selected_file'] = prepared['selected_file']
        report['preparation'] = {key: value for key, value in engine.last_preparation.items()
                                 if key in ('piece_length', 'candidate_peers', 'duration_seconds', 'bitrate',
                                            'start_buffer', 'elapsed_seconds')}
        size = engine.active['meta'].files[prepared['file_index']]['length']
        report['file_mib'] = round(size / MIB, 1)
        report['from_start'] = read_for(prepared['url'], 0, arguments.seconds, arguments.limit_mib * MIB)
        report['after_seek'] = read_for(prepared['url'], int(size * .6), arguments.seconds / 2, arguments.limit_mib * MIB)
        stats = engine.stats()
        report['swarm'] = {key: stats.get(key) for key in (
            'connected_peers', 'unchoked_peers', 'seeding_peers', 'fast_extension_peers', 'snubbed_peers',
            'queued_peers', 'known_peers', 'swarm_rate', 'peer_bytes_received', 'wasted_bytes', 'hedged_blocks',
            'cancelled_requests', 'hash_failures', 'banned_peers', 'evicted_peers', 'redials', 'peer_errors',
            'pex_encryption_preferred_percent', 'verified_pieces') if key in stats}
        report['peer_error_details'] = stats.get('peer_error_details')
        report['buffer'] = stats.get('buffer')
        report['http'] = stats.get('http')
    except Exception as error:
        report.update(error=type(error).__name__ + ': ' + str(error)[:200],
                      failed_after_seconds=round(time.monotonic() - started, 2),
                      preparation=dict(getattr(engine, 'last_preparation', {})),
                      metadata=dict(getattr(engine, 'last_metadata_diagnostics', {})))
    finally:
        engine.stop()
    report['stages'] = stages[:12]
    print(json.dumps(report, indent=1, default=str))


if __name__ == '__main__':
    main()
