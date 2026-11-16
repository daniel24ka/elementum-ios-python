"""Elementum view, provider and playback API implemented inside Kodi's Python."""

import base64
import json
import os
import re
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

from .catalog import Catalog, CatalogError, image, list_item, media_item, plugin_url, view
from .core import TorrentError
from .engine import TorrentEngine


class Redirect:
    def __init__(self, url, status=302):
        self.url = url
        self.status = status


class Backend:
    def __init__(self, profile, kodi, port=65220, catalog=None, engine=None):
        self.profile = profile
        self.kodi = kodi
        self.port = port
        self.base_url = 'http://127.0.0.1:' + str(port)
        self.catalog = catalog or Catalog(os.path.join(profile, 'python-catalog'),
                                          kodi.setting('tmdb_api_key'), kodi.language())
        self.engine = engine or TorrentEngine(profile, kodi.setting, kodi.progress, kodi.choose_file)
        self.callbacks = {}
        self.callback_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.state_path = os.path.join(profile, 'python-state.json')
        self.state = {'history': [], 'searches': [], 'watched': [], 'library': []}
        self.shutdown = threading.Event()
        self.last_info = {}
        self.pending_resume = 0
        os.makedirs(profile, exist_ok=True)
        try:
            with open(self.state_path, encoding='utf-8') as handle:
                loaded = json.load(handle)
                for key in self.state:
                    if isinstance(loaded.get(key), list):
                        self.state[key] = loaded[key]
        except (OSError, ValueError, AttributeError):
            pass

    def save(self):
        with self.state_lock:
            temporary = self.state_path + '.tmp'
            with open(temporary, 'w', encoding='utf-8') as handle:
                json.dump(self.state, handle)
            os.replace(temporary, self.state_path)

    def providers(self):
        return [addon for addon in self.kodi.addons()
                if re.fullmatch(r'script\.elementum\.[a-zA-Z0-9_.-]+', addon.get('addonid', ''))
                and addon.get('enabled')]

    def search_providers(self, method, search_object, timeout=35):
        providers = self.providers()
        if not providers:
            raise TorrentError('Enable Elementum Burst under Program add-ons to search for sources')
        registrations = []
        try:
            for addon in providers[:8]:
                token = secrets.token_hex(24)
                entry = {'ready': threading.Event(), 'results': []}
                with self.callback_lock:
                    self.callbacks[token] = entry
                payload = dict(search_object)
                payload['elementum_url'] = self.base_url
                payload.setdefault('proxy_url', '')
                payload.setdefault('internal_proxy_url', '')
                request = {'method': method, 'callback_url': self.base_url + '/callbacks/' + token,
                           'search_object': payload, 'callback_login': '', 'callback_password': ''}
                encoded = base64.b64encode(json.dumps(request).encode('utf-8')).decode('ascii')
                self.kodi.execute_addon(addon['addonid'], [encoded])
                registrations.append((token, entry))
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if self.kodi.abort_requested():
                    raise TorrentError('Search cancelled')
                if all(entry['ready'].is_set() for _, entry in registrations):
                    break
                self.shutdown.wait(0.1)
                if self.shutdown.is_set():
                    raise TorrentError('Service is closing')
            combined = []
            seen = set()
            for _, entry in registrations:
                for result in entry['results']:
                    if not isinstance(result, dict):
                        continue
                    uri = result.get('uri', '')
                    if not isinstance(uri, str) or len(uri) > 65536 or not uri.startswith(('magnet:?', 'http://', 'https://')):
                        continue
                    key = result.get('info_hash') or uri
                    if not isinstance(key, str) or key in seen:
                        continue
                    seen.add(key)
                    combined.append(result)
            combined.sort(key=lambda item: (self.number(item.get('sort_balance')), self.number(item.get('seeds'))), reverse=True)
            return combined[:200]
        finally:
            with self.callback_lock:
                for token, _ in registrations:
                    self.callbacks.pop(token, None)

    @staticmethod
    def number(value):
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def callback(self, token, results):
        with self.callback_lock:
            entry = self.callbacks.get(token)
            if entry is None or entry['ready'].is_set() or not isinstance(results, list):
                return False
            entry['results'] = results[:1000]
            entry['ready'].set()
            return True

    def links(self, method, payload, title, params=None):
        self.kodi.progress('Searching Elementum providers')
        results = self.search_providers(method, payload)
        items = []
        for result in results:
            item = list_item(result.get('name') or title,
                             plugin_url('/play', uri=result['uri'], query=title, **(params or {})),
                             playable=True, label2='{} seeds | {} | {}'.format(
                                 self.number(result.get('seeds')), result.get('size', ''), result.get('provider', '')))
            item['info'] = {'title': result.get('name') or title, 'plot': item['label2'], 'mediatype': 'video'}
            items.append(item)
        if not items:
            self.kodi.notify('No links were found')
        return view('files', items)

    def catalog_list(self, path, query):
        kind = 'movie' if path.startswith('/movies') else 'tv'
        base = '/movies' if kind == 'movie' else '/shows'
        suffix = path[len(base):].strip('/')
        page = min(500, max(1, self.number(query.get('page')) or 1))
        if not suffix:
            return view('', [list_item(label, plugin_url(base + route)) for label, route in (
                ('Search', '/search'), ('Popular', '/popular'), ('Top rated', '/top'),
                ('Recent', '/recent'), ('Trending', '/trending'), ('Genres', '/genres'), ('Library', '/library'))])
        if suffix == 'library':
            return view('movies' if kind == 'movie' else 'tvshows',
                        [media_item(entry['metadata'], kind) for entry in self.state['library'] if entry['kind'] == kind])
        if suffix == 'genres':
            genres = self.catalog.api('/genre/{}/list'.format(kind)).get('genres', [])
            return view('', [list_item(genre['name'], plugin_url(base + '/popular/genre/' + str(genre['id'])))
                             for genre in genres])
        params = {'page': page}
        if suffix == 'search':
            term = query.get('q') or self.kodi.input('Search movies' if kind == 'movie' else 'Search TV shows')
            if not term:
                return view('movies' if kind == 'movie' else 'tvshows', [])
            params['query'] = term
            query = dict(query, q=term)
            endpoint = '/search/' + kind
        elif suffix == 'trending':
            endpoint = '/trending/{}/week'.format(kind)
        elif suffix.startswith('popular/genre/'):
            endpoint = '/discover/' + kind
            params.update({'with_genres': suffix.rsplit('/', 1)[1], 'sort_by': 'popularity.desc'})
        else:
            category = {'popular': 'popular', 'top': 'top_rated',
                        'recent': 'now_playing' if kind == 'movie' else 'on_the_air'}.get(suffix)
            if not category:
                raise NotImplementedError('Catalog category is not implemented')
            endpoint = '/{}/{}'.format(kind, category)
        response = self.catalog.api(endpoint, params)
        items = [media_item(item, kind) for item in response.get('results', [])]
        if page < response.get('total_pages', 1) and page < 500:
            items.append(list_item('LOCALIZE[30218]', plugin_url(path, **dict(query, page=page + 1))))
        return view('movies' if kind == 'movie' else 'tvshows', items)

    def tv(self, match, query):
        identifier, season, episode, action = match.groups()
        show = self.catalog.details('tv', identifier)
        if action == 'seasons' or season is None:
            return view('seasons', [list_item(
                entry.get('name') or 'Season {}'.format(entry['season_number']),
                plugin_url('/show/{}/season/{}/episodes'.format(identifier, entry['season_number'])),
                info={'title': entry.get('name') or '', 'season': entry['season_number'],
                      'tvshowtitle': show.get('name') or '', 'mediatype': 'season'},
                poster=image(entry.get('poster_path') or show.get('poster_path')))
                for entry in show.get('seasons', [])])
        if episode is None:
            response = self.catalog.api('/tv/{}/season/{}'.format(identifier, season))
            return view('episodes', [list_item(
                '{}. {}'.format(entry['episode_number'], entry.get('name') or ''),
                plugin_url('/show/{}/season/{}/episode/{}/links'.format(identifier, season, entry['episode_number'])),
                info={'title': entry.get('name') or '', 'plot': entry.get('overview') or '',
                      'season': int(season), 'episode': entry['episode_number'],
                      'tvshowtitle': show.get('name') or '', 'mediatype': 'episode',
                      'premiered': entry.get('air_date') or ''},
                poster=image(entry.get('still_path') or show.get('poster_path')))
                for entry in response.get('episodes', [])])
        payload, _ = self.catalog.search_payload('tv', identifier, season, episode)
        details = self.catalog.api('/tv/{}/season/{}/episode/{}'.format(identifier, season, episode))
        payload['tmdb_id'] = details['id']
        payload['year'] = self.number((details.get('air_date') or '')[:4]) or payload['year']
        if action == 'infolabels':
            result = media_item(show, 'tv')
            result['info'].update({'title': details.get('name') or '', 'season': int(season),
                                    'episode': int(episode), 'mediatype': 'episode',
                                    'tvshowtitle': show.get('name') or '', 'plot': details.get('overview') or ''})
            return result
        title = '{} S{:02d}E{:02d}'.format(show.get('name') or payload['title'], int(season), int(episode))
        return self.links('search_episode', payload, title,
                          {'show': identifier, 'season': season, 'episode': episode, 'type': 'episode'})

    def route(self, path, query):
        path = path.rstrip('/') or '/'
        if path == '/':
            return view('', [list_item(label, plugin_url(route), action=action) for label, route, action in (
                ('LOCALIZE[30214]', '/movies/', False), ('LOCALIZE[30215]', '/shows/', False),
                ('LOCALIZE[30209]', '/search', False), ('LOCALIZE[30229]', '/torrents/', False),
                ('LOCALIZE[30216]', '/playtorrent', True), ('LOCALIZE[30537]', '/history/', False),
                ('LOCALIZE[30239]', '/provider/', False), ('LOCALIZE[30393]', '/status', True),
                ('LOCALIZE[30579]', '/settings/plugin.video.elementum', True))])
        if path == '/versions':
            return {'version': '0.2.0-python-development', 'user_agent': 'ElementumPython/0.2',
                    'backend': 'python', 'native_engine': False}
        if path.startswith(('/movies', '/shows')):
            return self.catalog_list(path, query)
        if path == '/search':
            term = query.get('q') or self.kodi.input('Elementum search')
            if not term:
                return view('files', [])
            self.state['searches'] = [term] + [value for value in self.state['searches'] if value != term][:49]
            self.save()
            return self.links('search', {'query': term, 'titles': {'original': term, 'source': term}}, term)
        movie = re.fullmatch(r'/movie/(\d+)/(links|play|forceplay|forcelinks|infolabels)', path)
        if movie:
            identifier, action = movie.groups()
            payload, details = self.catalog.search_payload('movie', identifier)
            if action == 'infolabels':
                return media_item(details, 'movie')
            return self.links('search_movie', payload, details.get('title') or payload['title'],
                              {'tmdb': identifier, 'type': 'movie'})
        show = re.fullmatch(r'/show/(\d+)(?:/season/(\d+))?(?:/episode/(\d+))?/(seasons|episodes|links|play|infolabels)', path)
        if show:
            return self.tv(show, query)
        if path == '/infolabels' or path.startswith('/search/infolabels/'):
            return self.last_info
        if path == '/playtorrent':
            uri = self.kodi.input('Magnet link or .torrent URL')
            return Redirect(plugin_url('/play', uri=uri), status=300) if uri else view('', [])
        if path == '/play':
            uri = query.get('uri', '')
            if not uri:
                raise TorrentError('No torrent URI supplied')
            title = query.get('query') or ''
            previous = next((entry for entry in self.state['history'] if entry['uri'] == uri), None)
            self.pending_resume = 0
            if previous and previous.get('position', 0) > 15 and self.kodi.resume(previous['position']):
                self.pending_resume = previous['position']
            self.last_info = {'label': title, 'label2': '', 'info': {'title': title, 'mediatype': 'video'}, 'art': {}}
            selected = self.number(query['oindex']) if 'oindex' in query else None
            try:
                prepared = self.engine.prepare(uri, selected, title, peer_only=query.get('peer_only') == '1')
            finally:
                self.kodi.progress_close()
            history = {'uri': uri, 'title': prepared['title'], 'file_index': prepared['file_index'],
                       'when': time.time(), 'position': 0, 'duration': 0}
            if previous:
                history['position'] = previous.get('position', 0)
                history['duration'] = previous.get('duration', 0)
            self.state['history'] = [history] + [entry for entry in self.state['history'] if entry['uri'] != uri][:99]
            self.save()
            return Redirect(prepared['url'])
        if path == '/history':
            return view('files', [list_item(entry['title'], plugin_url('/play', uri=entry['uri'], query=entry['title'],
                                                                      oindex=entry['file_index']), playable=True)
                                  for entry in self.state['history']])
        if path == '/history/clear':
            self.state['history'] = []
            self.save()
            return view('', [])
        if path in ('/torrents', '/torrents/list'):
            stats = self.engine.stats()
            if path.endswith('/list'):
                return [stats] if stats['active'] else []
            return view('files', [list_item(stats['title'], plugin_url('/torrents/stop'), action=True,
                                            label2='{} peers | {} verified pieces'.format(
                                                stats.get('connected_peers', 0), stats.get('verified_pieces', 0)))]
                        if stats['active'] else [])
        if path == '/torrents/stop':
            self.engine.stop()
            return view('', [])
        if path == '/provider':
            return view('', [list_item(addon.get('name') or addon['addonid'],
                                      plugin_url('/settings/' + addon['addonid']), action=True)
                             for addon in self.providers()])
        library = re.fullmatch(r'/library/(movie|tv)/add/(\d+)', path)
        if library:
            kind, identifier = library.groups()
            metadata = self.catalog.details(kind, identifier)
            entry = {'kind': kind, 'id': int(identifier), 'metadata': metadata}
            self.state['library'] = [entry] + [item for item in self.state['library']
                                              if not (item['kind'] == kind and item['id'] == int(identifier))]
            folder = os.path.join(self.profile, 'library', kind)
            os.makedirs(folder, exist_ok=True)
            if kind == 'movie':
                with open(os.path.join(folder, identifier + '.strm'), 'w', encoding='utf-8') as handle:
                    handle.write(plugin_url('/movie/{}/play'.format(identifier)) + '\n')
            self.save()
            self.kodi.notify('Added to Elementum library')
            return view('', [])
        if path.startswith('/settings/'):
            addon_id = path.split('/')[-1]
            if not re.fullmatch(r'(plugin\.video\.elementum|script\.elementum\.[a-zA-Z0-9_.-]+)', addon_id):
                raise TorrentError('Invalid settings target')
            self.kodi.open_settings(addon_id)
            return view('', [])
        if path == '/status':
            self.kodi.status(self.engine.stats())
            return view('', [])
        if path in ('/reload', '/restart', '/notification', '/cmd/clear_page_cache'):
            return {'status': 'ok'}
        if path == '/shutdown':
            self.shutdown.set()
            return {'status': 'closing'}
        if path.startswith('/provider/') and path.endswith('/failure'):
            return {'status': 'recorded'}
        raise NotImplementedError('This Elementum endpoint has not been ported to Python: ' + path)


class BackendServer:
    def __init__(self, backend):
        self.backend = backend
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def send_json(self, value, code=200):
                body = json.dumps(value, ensure_ascii=False).encode('utf-8')
                self.send_response(code)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Connection', 'close')
                self.end_headers()
                self.close_connection = True
                self.wfile.write(body)

            def do_POST(self):
                token = self.path[len('/callbacks/'):]
                if not self.path.startswith('/callbacks/') or not re.fullmatch(r'[a-f0-9]{48}', token):
                    self.send_json({'error': 'not found'}, 404)
                    return
                try:
                    length = int(self.headers.get('Content-Length', '0'))
                    if not 0 < length <= 4 * 1024 * 1024:
                        raise ValueError('Invalid callback length')
                    payload = json.loads(self.rfile.read(length))
                    accepted = owner.backend.callback(token, payload)
                    self.send_json({'accepted': accepted}, 200 if accepted else 410)
                except (ValueError, TypeError):
                    self.send_json({'error': 'invalid callback'}, 400)

            def do_GET(self):
                try:
                    parsed = urlsplit(self.path)
                    query = {key: values[-1] for key, values in parse_qs(parsed.query, max_num_fields=64).items()}
                    result = owner.backend.route(unquote(parsed.path), query)
                    if isinstance(result, Redirect):
                        self.send_response(result.status)
                        self.send_header('Location', result.url)
                        self.send_header('Content-Length', '0')
                        self.send_header('Connection', 'close')
                        self.end_headers()
                        self.close_connection = True
                    else:
                        self.send_json(result)
                except NotImplementedError as error:
                    self.send_json({'error': str(error)}, 501)
                except (TorrentError, CatalogError, OSError, ValueError) as error:
                    owner.backend.kodi.error(type(error).__name__, str(error))
                    owner.backend.kodi.progress_close()
                    self.send_json({'error': type(error).__name__, 'message': str(error)}, 503)
                except Exception as error:
                    owner.backend.kodi.error(type(error).__name__, 'Backend request failed')
                    owner.backend.kodi.progress_close()
                    self.send_json({'error': type(error).__name__, 'message': 'Backend request failed'}, 500)

        class Server(ThreadingHTTPServer):
            daemon_threads = True

        self.httpd = Server(('127.0.0.1', backend.port), Handler)
        backend.port = self.httpd.server_port
        backend.base_url = 'http://127.0.0.1:' + str(backend.port)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.backend.shutdown.set()
        self.backend.engine.stop()
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=3)
