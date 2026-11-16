"""Read a video's duration from the verified first and last pieces.

Knowing the duration before playback gives the real average bitrate, so the
start buffer and the read-ahead window are sized for this film instead of a
guess. Only two fields are read; anything unexpected simply returns None.
"""
import struct

LIMIT = 4 * 1024 * 1024


def _vint(data, position):
    """EBML variable-length size: (value, next position) or (None, position)."""
    if position >= len(data):
        return None, position
    first = data[position]
    if not first:
        return None, position
    length = 9 - first.bit_length()
    if position + length > len(data):
        return None, position
    value = first & (0xFF >> length)
    for offset in range(1, length):
        value = (value << 8) | data[position + offset]
    if value == (1 << (7 * length)) - 1:
        value = -1  # Unknown size: the element runs to the end of its parent.
    return value, position + length


def _element_id(data, position):
    if position >= len(data) or not data[position]:
        return None, position
    length = 9 - data[position].bit_length()
    if length > 4 or position + length > len(data):
        return None, position
    return bytes(data[position:position + length]), position + length


def matroska_duration(head):
    """Seconds from Segment/Info: Duration (0x4489) x TimecodeScale (0x2AD7B1)."""
    start = 0
    # The real Info element is near the front and has a dozen children. Bounding
    # both loops keeps crafted input (the ID repeated thousands of times, each
    # with an unknown size) from costing more than a few thousand steps.
    for _ in range(16):
        found = head.find(b'\x15\x49\xa9\x66', start)
        if found < 0:
            return None
        start = found + 4
        size, position = _vint(head, found + 4)
        if size is None:
            continue
        end = len(head) if size < 0 else min(len(head), position + size)
        scale, duration = 1000000, None
        for _ in range(256):
            if position >= end:
                break
            identifier, position = _element_id(head, position)
            if identifier is None:
                break
            size, position = _vint(head, position)
            if size is None or size < 0 or position + size > end:
                break
            if identifier == b'\x2a\xd7\xb1' and 1 <= size <= 8:
                scale = int.from_bytes(head[position:position + size], 'big')
            elif identifier == b'\x44\x89' and size in (4, 8):
                duration = struct.unpack('>f' if size == 4 else '>d', bytes(head[position:position + size]))[0]
            position += size
        if duration and scale:
            return duration * scale / 1e9
    return None


def mp4_duration(*regions):
    """Seconds from the movie header box, wherever the moov box was written."""
    for data in regions:
        start = 0
        for _ in range(16):
            found = data.find(b'mvhd', start)
            if found < 4:
                if found < 0:
                    break
                start = found + 4
                continue
            start = found + 4
            fields = bytes(data[found + 4:found + 36])
            if len(fields) < 32:
                continue
            if fields[0] == 1:
                timescale, duration = struct.unpack('>IQ', fields[20:32])
            elif fields[0] == 0:
                timescale, duration = struct.unpack('>II', fields[12:20])
            else:
                continue
            if timescale and duration and duration != 0xFFFFFFFF:
                return duration / timescale
    return None


def mp4_index_offset(head, length):
    """File offset of a moov box written after the media data, else None."""
    position = 0
    for _ in range(64):   # A real file has a handful of top-level boxes before its media.
        if position + 8 > len(head):
            break
        size, kind = struct.unpack('>I4s', bytes(head[position:position + 8]))
        if kind == b'moov':
            return None            # Fast-start file: the index is already at the front.
        if size == 1:
            if position + 16 > len(head):
                return None
            size = struct.unpack('>Q', bytes(head[position + 8:position + 16]))[0]
        if size < 8 or not kind.isalnum():
            return None
        position += size
    return position if 0 < position < length - 8 else None


def matroska_index_offset(head, length):
    """File offset of the Cues element named by the SeekHead, else None."""
    segment = head.find(b'\x18\x53\x80\x67')
    if segment < 0:
        return None
    _, origin = _vint(head, segment + 4)
    found = head.find(b'\x11\x4d\x9b\x74', origin)
    if found < 0:
        return None
    size, position = _vint(head, found + 4)
    if size is None or size < 0:
        return None
    end = min(len(head), position + size)
    for _ in range(64):   # A SeekHead lists a few top-level elements.
        if position >= end:
            break
        identifier, position = _element_id(head, position)
        if identifier is None:
            return None
        size, position = _vint(head, position)
        if size is None or size < 0 or position + size > end:
            return None
        if identifier == b'\x4d\xbb':
            target, offset, inner, stop = None, None, position, position + size
            for _ in range(8):
                if inner >= stop:
                    break
                child, inner = _element_id(head, inner)
                if child is None:
                    break
                child_size, inner = _vint(head, inner)
                if child_size is None or child_size < 0 or inner + child_size > stop:
                    break
                if child == b'\x53\xab':
                    target = bytes(head[inner:inner + child_size])
                elif child == b'\x53\xac' and 1 <= child_size <= 8:
                    offset = int.from_bytes(head[inner:inner + child_size], 'big')
                inner += child_size
            if target == b'\x1c\x53\xbb\x6b' and offset is not None:
                absolute = origin + offset
                return absolute if 0 < absolute < length - 8 else None
        position += size
    return None


def starts_index(data, extension):
    """True when these bytes begin with the element index_offset() pointed at."""
    data = bytes(data[:8])
    if extension in ('.mp4', '.m4v', '.mov'):
        return data[4:8] == b'moov'
    return data[:4] == b'\x1c\x53\xbb\x6b'


def index_offset(head, length, extension):
    """Where the player will jump to read the seek index right after opening.

    MP4 files not prepared for streaming keep their whole index (moov) after
    the media; Matroska files usually keep their Cues near the end. Fetching
    that region while the start buffer fills saves the player's first seek.
    """
    try:
        head = bytes(head[:LIMIT])
        if extension in ('.mp4', '.m4v', '.mov'):
            return mp4_index_offset(head, length)
        if extension in ('.mkv', '.webm'):
            return matroska_index_offset(head, length)
    except (struct.error, ValueError, IndexError, TypeError, OverflowError, AttributeError):
        pass
    return None


def duration_seconds(head, tail, extension):
    """Duration of a Matroska/WebM or MP4/MOV file, or None when it is not stated."""
    try:
        # Bounded copies: the pieces may be memoryviews of buffers up to 64 MiB.
        head, tail = bytes(head[:LIMIT]), bytes(tail[-LIMIT:])
        if extension in ('.mkv', '.webm'):
            seconds = matroska_duration(head)
        elif extension in ('.mp4', '.m4v', '.mov'):
            seconds = mp4_duration(head, tail)
        else:
            seconds = None
        return seconds if seconds and 30 <= seconds <= 24 * 3600 else None
    except (struct.error, ValueError, IndexError, TypeError, OverflowError, AttributeError):
        return None
