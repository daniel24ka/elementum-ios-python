"""Bounded streaming decisions, independent of Kodi and network transports."""
import math

MIB = 1024 * 1024

def read_ahead_pieces(piece_length, cache_pieces, bitrate=0, network_rate=0, caching=False):
    # Keep the current piece and one seek/back-read slot. Very large pieces do
    # not allocate a second speculative copy just to reach a time target.
    capacity = max(0, cache_pieces - 2)
    if not capacity:
        return 0
    seconds = 30 if not network_rate or not bitrate or network_rate >= bitrate * 1.25 else 60
    if caching:
        seconds = max(seconds, 45)
    desired = bitrate * seconds if bitrate else 8 * MIB
    return min(capacity, max(1, int(math.ceil(desired / piece_length))))

class StallGuard:
    """Recover only a persistent empty-buffer stall, never a pause or a seek."""
    def __init__(self, timeout=25):
        self.timeout = timeout
        self.url = None
        self.position = 0
        self.bytes = 0
        self.changed = 0
        self.data_changed = 0
        self.sent = False

    def sample(self, url, position, duration, received, now, paused=False, caching=False, failed=False):
        if url != self.url:
            self.url, self.position, self.bytes = url, position, received
            self.changed = self.data_changed = now
            self.sent = False
            return False
        if paused or abs(position - self.position) > .05:
            self.changed = now
        if received > self.bytes:
            self.data_changed = now
        self.position, self.bytes = position, received
        if paused or self.sent or position < 15 or (duration and duration - position < 30):
            return False
        if failed or (caching and now - self.changed >= self.timeout and now - self.data_changed >= 15):
            self.sent = True
            return True
        return False

def premature_end(position, duration):
    """True when playback "ended" well before the end: starvation, not the credits."""
    return bool(duration and position >= 15 and duration - position > max(60, duration * .03))

def kodi_cache_target(free_mib, current_mib):
    # Zero explicitly means a full-file disk cache; never replace that choice.
    if current_mib == 0:
        return current_mib
    target = 64 if free_mib >= 512 else 32 if free_mib >= 256 else 20
    return max(current_mib, target)

def prebuffer_target(file_length, cache_bytes, piece_length, wanted=8 * MIB, bitrate=0):
    """Verified bytes to hold at the file start before the player is given the URL.

    About eight seconds of this video when its bitrate is known, never more than
    the configured size or a quarter of the engine cache, and never a second
    copy of a very large piece: the first piece is already verified by now.
    """
    if wanted <= 0:
        return 0
    target = min(wanted, cache_bytes // 4, file_length)
    if bitrate > 0:
        target = min(target, max(MIB, int(bitrate * 8)))
    return 0 if target <= piece_length else target

def prebuffer_done(ahead, target, rate=0, bitrate=0, waited=0):
    """Stop buffering early when the swarm is clearly faster than the video."""
    if ahead >= target:
        return True
    return bool(bitrate > 0 and waited >= 1 and rate >= bitrate * 1.5 and ahead >= bitrate * 2)

FAILURES = (
    ('cancelled', ('cancel',)),
    ('no_video', ('no video files', 'file selection', 'invalid selected')),
    ('integrity', ('sha-1', 'verification failed', 'info-hash')),
    ('no_peers', ('no torrent peers found', 'no usable torrent peers')),
    ('metadata', ('none supplied usable torrent metadata',)),
    ('peers_unreachable', ('no usable tcp peers', 'no connected peer supplied')),
    ('peers_busy', ('upload slot',)),
    ('missing_pieces', ('has this part',)),
    ('timeout', ('did not buffer in time', 'timed out')),
)

def classify_failure(message, stage=''):
    """A stable reason code for diagnostics.

    The stage only labels an otherwise unknown failure in diagnostics; it never
    selects the advice shown to the viewer (see user_message).
    """
    text = str(message or '').lower()
    for code, needles in FAILURES:
        if any(needle in text for needle in needles):
            return code
    return 'metadata' if stage == 'metadata' else 'unknown'

ADVICE = {
    'no_peers': 'No peers were found for this source. Try another source.',
    'metadata': 'Peers were found but none sent the torrent details. Try another source.',
    'peers_unreachable': 'Peers were found but none accepted a connection. Try another source.',
    'peers_busy': 'Peers are connected but busy; none granted an upload slot in time. Try another source.',
    'missing_pieces': 'The connected peers do not have this part of the video yet. Try another source.',
    'timeout': 'This source is too slow to buffer in time. Try another source.',
    'integrity': 'This source sent data that failed verification. Try another source.',
    'no_video': 'This torrent has no playable video file.',
}

def user_message(message):
    """Plain advice for a recognised failure; the original text for anything else.

    Input errors ("Use an HTTP(S) torrent URL or a magnet link") are raised
    before any peer is contacted and must reach the viewer unchanged.
    """
    return ADVICE.get(classify_failure(message)) or str(message)
