"""Release and show identifiers for subtitle matching, without extra peer reads."""
import json
import re
import struct

def _cached(store, meta, file_index, offset, length):
    position = meta.files[file_index]['offset'] + offset
    parts = []
    with store.lock:
        while length:
            index, inside = divmod(position, meta.piece_length)
            piece = store.cache.get(index)
            if piece is None:
                return None
            size = min(length, len(piece) - inside)
            if size <= 0:
                return None
            parts.append(piece[inside:inside + size])
            position, length = position + size, length - size
    return b''.join(parts)

def build_context(session, url):
    metadata = session.get('metadata') or {}
    info, properties = metadata.get('info', {}), metadata.get('properties', {})
    query = session.get('playback_query') or {}
    episode = info.get('mediatype') == 'episode' or bool(query.get('show'))
    identifiers = metadata.get('uniqueids') or {}
    if episode:
        try:
            identifiers = json.loads(properties.get('script.trakt.ids') or '{}')
        except (ValueError, TypeError):
            identifiers = {}
    imdb = properties.get('tvshow.imdb_id') if episode else identifiers.get('imdb')
    imdb = imdb or identifiers.get('imdb') or ''
    title = (info.get('tvshowtitle') if episode else info.get('originaltitle')) or info.get('title') or session.get('title') or ''
    valid_imdb = bool(re.fullmatch(r'tt\d{5,12}', str(imdb)))
    result = {'path': url, 'kind': 'series' if episode else 'movie', 'title': title, 'year': info.get('year') or '',
              'ids': [imdb] if valid_imdb else [], 'lookup': None if valid_imdb else (title, info.get('year') or '')}
    if episode:
        result.update(season=int(query.get('season') or info.get('season') or 0),
                      episode=int(query.get('episode') or info.get('episode') or 0))
    meta, index = session.get('meta'), session.get('file_index')
    if meta is not None and type(index) is int:
        item = meta.files[index]
        result.update(release=item['path'][-1], videoSize=item['length'])
        store = session.get('store')
        if store and item['length'] >= 131072:
            head = _cached(store, meta, index, 0, 65536)
            tail = _cached(store, meta, index, item['length'] - 65536, 65536)
            if head is not None and tail is not None:
                total = item['length'] + sum(struct.unpack('<8192Q', head)) + sum(struct.unpack('<8192Q', tail))
                result['videoHash'] = '{:016x}'.format(total & 0xFFFFFFFFFFFFFFFF)
    return result
