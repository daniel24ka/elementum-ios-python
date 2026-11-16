"""TMDB catalog adapter for Elementum's existing Kodi view format."""

import hashlib
import json
import os
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from .defaults import TMDB_KEYS
except ImportError:
    TMDB_KEYS = ()


class CatalogError(Exception):
    pass


class Catalog:
    def __init__(self, cache_dir, key='', language='en-US', transport=None):
        self.cache_dir = cache_dir
        self.keys = list(dict.fromkeys(([key] if key else []) + list(TMDB_KEYS)))
        self.language = language
        self.transport = transport
        self.lock = threading.Lock()
        os.makedirs(cache_dir, exist_ok=True)

    def api(self, path, params=None, ttl=600):
        if not path.startswith('/') or '://' in path or '..' in path:
            raise CatalogError('Invalid catalog path')
        params = dict(params or {})
        params.setdefault('language', self.language)
        digest = hashlib.sha256((path + json.dumps(params, sort_keys=True)).encode('utf-8')).hexdigest()
        filename = os.path.join(self.cache_dir, digest + '.json')
        try:
            if time.time() - os.path.getmtime(filename) < ttl:
                with open(filename, encoding='utf-8') as handle:
                    return json.load(handle)
        except (OSError, ValueError):
            pass
        if self.transport:
            result = self.transport(path, params)
        else:
            if not self.keys:
                raise CatalogError('A TMDB API key is required in Elementum settings')
            result = None
            for key in self.keys:
                query = dict(params, api_key=key)
                request = Request('https://api.themoviedb.org/3' + path + '?' + urlencode(query),
                                  headers={'Accept': 'application/json', 'User-Agent': 'ElementumPython/0.2'})
                try:
                    with urlopen(request, timeout=12) as response:
                        content = response.read(4 * 1024 * 1024 + 1)
                    if len(content) > 4 * 1024 * 1024:
                        raise CatalogError('Catalog response too large')
                    result = json.loads(content)
                    break
                except HTTPError as error:
                    status = error.code
                    error.close()
                    if status not in (401, 403):
                        raise CatalogError('TMDB request failed: HTTP {}'.format(status)) from None
                except (OSError, ValueError):
                    raise CatalogError('TMDB connection or response failed') from None
            if result is None:
                raise CatalogError('TMDB authentication failed; update the API key in settings')
        with self.lock:
            temporary = filename + '.tmp'
            with open(temporary, 'w', encoding='utf-8') as handle:
                json.dump(result, handle)
            os.replace(temporary, filename)
        return result

    def details(self, kind, identifier):
        if kind not in ('movie', 'tv') or not str(identifier).isdigit():
            raise CatalogError('Invalid catalog identifier')
        return self.api('/{}/{}'.format(kind, identifier),
                        {'append_to_response': 'external_ids,credits,alternative_titles'}, ttl=86400)

    def search_payload(self, kind, identifier, season=None, episode=None):
        item = self.details(kind, identifier)
        original = item.get('original_title') or item.get('original_name') or ''
        localized = item.get('title') or item.get('name') or original
        date = item.get('release_date') or item.get('first_air_date') or ''
        year = int(date[:4]) if date[:4].isdigit() else 0
        titles = {'original': original, 'source': original, 'en': original,
                  self.language[:2].lower(): localized}
        alternate = item.get('alternative_titles', {})
        for title in alternate.get('titles', alternate.get('results', [])):
            country = title.get('iso_3166_1', '').lower()
            if country and title.get('title'):
                titles.setdefault(country, title['title'])
        external = item.get('external_ids', {})
        payload = {'title': original, 'titles': titles, 'year': year,
                   'years': {'original': year}, 'imdb_id': item.get('imdb_id') or external.get('imdb_id') or '',
                   'tmdb_id': int(identifier), 'silent': False, 'skip_auth': False,
                   'proxy_url': '', 'internal_proxy_url': '', 'elementum_url': ''}
        if kind == 'tv':
            payload.update({'show_tmdb_id': int(identifier), 'tvdb_id': external.get('tvdb_id') or 0,
                            'season': int(season or 0), 'show_year': year, 'season_year': year,
                            'anime': False, 'absolute_number': 0, 'season_name': ''})
            if episode is not None:
                payload['episode'] = int(episode)
        return payload, item


def plugin_url(path, **params):
    return 'plugin://plugin.video.elementum' + path + ('?' + urlencode(params) if params else '')


def view(content, items):
    return {'content_type': content, 'items': items}


def list_item(label, path, playable=False, action=False, info=None, poster='', fanart='', label2=''):
    return {'label': str(label), 'label2': str(label2), 'icon': '', 'thumbnail': poster,
            'is_playable': bool(playable), 'is_action': bool(action), 'path': path,
            'info': info or {}, 'art': {'poster': poster, 'thumb': poster, 'fanart': fanart}}


def image(path, size='w500'):
    return 'https://image.tmdb.org/t/p/' + size + path if path else ''


def media_item(item, kind):
    identifier = item['id']
    title = item.get('title') or item.get('name') or ''
    date = item.get('release_date') or item.get('first_air_date') or ''
    info = {'title': title, 'originaltitle': item.get('original_title') or item.get('original_name') or title,
            'plot': item.get('overview') or '', 'year': int(date[:4]) if date[:4].isdigit() else 0,
            'rating': item.get('vote_average') or 0, 'votes': str(item.get('vote_count') or 0),
            'mediatype': 'movie' if kind == 'movie' else 'tvshow',
            'premiered': date, 'duration': int(item.get('runtime') or 0) * 60,
            'genre': [genre['name'] for genre in item.get('genres', []) if genre.get('name')]}
    result = list_item(title, plugin_url('/movie/{}/links'.format(identifier)) if kind == 'movie'
                       else plugin_url('/show/{}/seasons'.format(identifier)),
                       info=info, poster=image(item.get('poster_path')), fanart=image(item.get('backdrop_path'), 'w1280'))
    result['uniqueids'] = {'tmdb': str(identifier)}
    if item.get('imdb_id'):
        result['uniqueids']['imdb'] = item['imdb_id']
    result['context_menu'] = [
        ['LOCALIZE[30579]', 'RunPlugin(' + plugin_url('/settings/plugin.video.elementum') + ')'],
        ['Add to library', 'RunPlugin(' + plugin_url('/library/{}/add/{}'.format(kind, identifier)) + ')']]
    return result
