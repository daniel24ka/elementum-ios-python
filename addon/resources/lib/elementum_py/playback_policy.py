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

def kodi_cache_target(free_mib, current_mib):
    # Zero explicitly means a full-file disk cache; never replace that choice.
    if current_mib == 0:
        return current_mib
    target = 64 if free_mib >= 512 else 32 if free_mib >= 256 else 20
    return max(current_mib, target)
