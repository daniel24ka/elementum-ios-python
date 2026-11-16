"""Elementum view, provider and playback API implemented inside Kodi's Python."""

import base64
import json
import os
import re
import secrets
import threading
import time
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

from .catalog import Catalog, CatalogError, image, list_item, media_item, plugin_url, view
from .core import TorrentError
from .engine import TorrentEngine
from .downloads import Downloads
from .version import VERSION
from .integrations import COMPANIONS, playback_metadata, upnext_payload, upnext_notification
from .source_health import SourceHealth, resolution


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
        self.downloads = Downloads(profile, kodi.setting)
        self.web = None
        self.play_lock = threading.Lock()
        self.source_groups = OrderedDict()
        self.source_lock = threading.Lock()
        self.last_recovery = {}
        self.callbacks = {}
        self.callback_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.state_path = os.path.join(profile, 'python-state.json')
        self.state = {'history': [], 'searches': [], 'watched': [], 'library': []}
        self.shutdown = threading.Event()
        self.last_info = {}
        self.pending_resume = 0
        self.playback_query = {}
        os.makedirs(profile, exist_ok=True)
        self.source_health = SourceHealth(profile)
        self.recovery_chains = OrderedDict()
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
        report = {'provider_count': len(providers), 'callbacks_received': 0, 'results': 0,
                  'timed_out': False, 'cancelled': False}
        self.last_search = report
        started = time.monotonic()
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
                    report['cancelled'] = True
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
            if self.kodi.setting('source_health') != 'false':
                combined = self.source_health.rank(combined)
            report['callbacks_received'] = sum(entry['ready'].is_set() for _, entry in registrations)
            report['results'] = len(combined[:200])
            report['timed_out'] = report['callbacks_received'] < len(registrations)
            if not combined and report['callbacks_received'] == 0:
                raise TorrentError('Source search timed out; check Burst and try again')
            return combined[:200]
        finally:
            report['elapsed_seconds'] = round(time.monotonic() - started, 2)
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
        try:
            results = self.search_providers(method, payload)
            self.remember_sources(title, params or {}, results)
            items = []
            for result in results:
                item = list_item(result.get('name') or title,
                                 plugin_url('/play', uri=result['uri'], query=title, **(params or {})),
                                 playable=True, label2='{} seeds | {} | {}'.format(
                                     self.number(result.get('seeds')), result.get('size', ''), result.get('provider', '')))
                item['info'] = {'title': result.get('name') or title, 'plot': item['label2'], 'mediatype': 'video'}
                item['context_menu'] = [['Download selected video', 'RunPlugin({})'.format(
                    plugin_url('/downloads/add', uri=result['uri'], query=title, **(params or {})))],
                    ['Download all files', 'RunPlugin({})'.format(
                    plugin_url('/downloads/add', uri=result['uri'], query=title, all_files='1', **(params or {})))]]
                items.append(item)
            if not items:
                health = getattr(self.kodi, 'provider_health', lambda: {})()
                if health.get('blocked_domains'):
                    self.kodi.notify('No sources returned. Some search sites require browser verification.')
                else:
                    self.kodi.notify('No sources returned for this title. Try another title or source.')
                self.last_search.update(health)
            return view('files', items)
        finally:
            self.kodi.progress_close()

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
            return playback_metadata(self.catalog, {'show': identifier, 'season': season, 'episode': episode})
        title = '{} S{:02d}E{:02d}'.format(show.get('name') or payload['title'], int(season), int(episode))
        if action == 'autoplay':
            self.kodi.progress('Finding sources for the next episode')
            try:
                sources = self.search_providers('search_episode', payload)
                if not sources:
                    raise TorrentError('No sources found for the next episode')
                return Redirect(plugin_url('/play', uri=sources[0]['uri'], query=title, show=identifier,
                                           season=season, episode=episode, type='episode'), status=300)
            finally:
                self.kodi.progress_close()
        return self.links('search_episode', payload, title,
                          {'show': identifier, 'season': season, 'episode': episode, 'type': 'episode'})

    def route(self, path, query):
        if self.shutdown.is_set():
            raise TorrentError('Elementum is closing; reopen it before trying again')
        path = path.rstrip('/') or '/'
        if path == '/':
            return view('', [list_item(label, plugin_url(route), action=action) for label, route, action in (
                ('LOCALIZE[30214]', '/movies/', False), ('LOCALIZE[30215]', '/shows/', False),
                ('LOCALIZE[30209]', '/search', False), ('LOCALIZE[30229]', '/torrents/', False),
                ('LOCALIZE[30216]', '/playtorrent', True), ('LOCALIZE[30537]', '/history/', False),
                ('LOCALIZE[30239]', '/provider/', False), ('LOCALIZE[30393]', '/status', True),
                ('Trakt, SIMKL and Up Next', '/integrations/', False),
                ('Downloads', '/downloads/', False), ('Web interface', '/web', True),
                ('Optimize playback buffer', '/playback/cache', True),
                ('LOCALIZE[30579]', '/settings/plugin.video.elementum', True))])
        if path == '/versions':
            return {'version': VERSION, 'user_agent': 'ElementumPython/0.3',
                    'backend': 'python', 'native_engine': False}
        if path == '/diagnostics':
            return {'torrent': self.engine.stats(), 'source_search': getattr(self, 'last_search', {}),
                    'preparation': getattr(self.engine, 'last_preparation', {}),
                    'last_failure': getattr(self, 'last_failure', {}),
                    'source_recovery': self.last_recovery,
                    'metadata': getattr(self.engine, 'last_metadata_diagnostics', {})}
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
                return playback_metadata(self.catalog, {'tmdb': identifier, 'type': 'movie'})
            if action in ('play', 'forceplay'):
                self.kodi.progress('Finding movie sources')
                try:
                    sources = self.search_providers('search_movie', payload)
                    self.remember_sources(details.get('title') or payload['title'],
                                          {'tmdb': identifier, 'type': 'movie'}, sources)
                    if not sources:
                        raise TorrentError('No sources found for this movie')
                    return Redirect(plugin_url('/play', uri=sources[0]['uri'],
                        query=details.get('title') or payload['title'], tmdb=identifier, type='movie'), status=300)
                finally:
                    self.kodi.progress_close()
            return self.links('search_movie', payload, details.get('title') or payload['title'],
                              {'tmdb': identifier, 'type': 'movie'})
        show = re.fullmatch(r'/show/(\d+)(?:/season/(\d+))?(?:/episode/(\d+))?/(seasons|episodes|links|play|autoplay|infolabels)', path)
        if show:
            return self.tv(show, query)
        if path == '/infolabels' or path.startswith('/search/infolabels/'):
            return self.last_info
        if path == '/playtorrent':
            uri = self.kodi.input('Magnet link or .torrent URL')
            return Redirect(plugin_url('/play', uri=uri), status=300) if uri else view('', [])
        if path == '/play':
            if not self.play_lock.acquire(blocking=False):
                raise TorrentError('Another video is being prepared; wait or cancel it first')
            try:
                return self.play(query)
            except (TorrentError, OSError, ValueError):
                self.last_failure = dict(getattr(self.engine, 'last_preparation', {}))
                raise
            finally:
                self.play_lock.release()
        if path == '/downloads' or path.startswith('/downloads/'):
            return self.download_route(path, query)
        if path == '/web':
            if self.web:
                self.kodi.web_access(self.web.address, self.web.token)
            else:
                self.kodi.notify('Enable the Web interface in Elementum settings and restart the add-on')
            return view('', [])
        if path == '/history':
            return view('files', [list_item(entry['title'],
                plugin_url('/downloads/{}/play'.format(entry['download']), oindex=entry['file_index'])
                if entry.get('download') else plugin_url('/play', uri=entry['uri'], query=entry['title'],
                oindex=entry['file_index'], **entry.get('media', {})), playable=True)
                for entry in self.state['history']])
        if path == '/history/clear':
            self.state['history'] = []
            self.save()
            return view('', [])
        return self.management_route(path, query)

    def play(self, query):
            uri = query.get('uri', '')
            if not uri:
                raise TorrentError('No torrent URI supplied')
            title = query.get('query') or ''
            selected = self.number(query['oindex']) if 'oindex' in query else None
            media = {key: query[key] for key in ('show', 'season', 'episode', 'tmdb', 'type') if key in query}
            previous = next((entry for entry in self.state['history'] if entry.get('uri') == uri
                and (entry.get('file_index') == selected if selected is not None else entry.get('media', {}) == media)), None)
            self.pending_resume = 0
            if query.get('resume') != '0' and previous and previous.get('position', 0) > 15 and self.kodi.resume(previous['position']):
                self.pending_resume = previous['position']
            chain = self.recovery_chains.get(query.get('_recovery', ''))
            if chain and chain.get('expected') != self.source_identity(uri):
                chain = None
            if chain and chain.get('expected') == self.source_identity(uri):
                self.pending_resume = chain['position']
            metadata = {'label': title, 'label2': '', 'info': {'title': title, 'mediatype': 'video'}, 'art': {}}
            try:
                metadata = playback_metadata(self.catalog, query) or metadata
            except (CatalogError, ValueError):
                pass
            playback_query = {key: query[key] for key in ('show', 'season', 'episode', 'tmdb', 'type') if key in query}
            if getattr(self.engine, 'profile', self.profile) != self.profile:
                self.engine.stop()
                self.engine = TorrentEngine(self.profile, self.kodi.setting, self.kodi.progress, self.kodi.choose_file)
            try:
                prepared, uri = self.prepare_with_recovery(uri, selected, title, query)
            finally:
                self.kodi.progress_close()
            self.last_info = metadata
            self.playback_query = playback_query
            session = self.engine.active
            if session and session['stream'].url == prepared['url']:
                session['playback_query'] = playback_query
                session['metadata'] = metadata
                chain_id = query.get('_recovery') if chain else secrets.token_hex(12)
                if not chain:
                    chain = {'attempted':set(), 'count':0, 'when':time.monotonic()}
                    self.recovery_chains[chain_id] = chain
                    while len(self.recovery_chains) > 4:
                        self.recovery_chains.popitem(last=False)
                chain['attempted'].add(self.source_identity(uri))
                session['recovery_chain'] = chain_id
                session['peer_only'] = query.get('peer_only') == '1'
                if chain.get('count'):
                    self.last_recovery['midstream'] = {'attempt':chain['count'],'resume_seconds':round(chain['position'],2)}
            history = {'uri': uri, 'title': prepared['title'], 'file_index': prepared['file_index'],
                       'when': time.time(), 'position': 0, 'duration': 0, 'media': playback_query}
            if previous:
                history['position'] = previous.get('position', 0)
                history['duration'] = previous.get('duration', 0)
            self.state['history'] = [history] + [entry for entry in self.state['history']
                if entry.get('uri') != uri or entry.get('file_index') != prepared['file_index']][:99]
            self.save()
            return Redirect(prepared['url'])

    @staticmethod
    def source_key(title, query):
        if query.get('show') and query.get('season') is not None and query.get('episode') is not None:
            return ('episode', str(query['show']), str(query['season']), str(query['episode']))
        if query.get('tmdb') and query.get('type') == 'movie':
            return ('movie', str(query['tmdb']))
        return ('search', title.strip().casefold())

    @staticmethod
    def source_identity(uri):
        if uri.startswith('magnet:?'):
            from .discovery import parse_magnet
            try:
                return parse_magnet(uri)['info_hash'].hex()
            except TorrentError:
                pass
        return uri

    def remember_sources(self, title, query, results):
        key = self.source_key(title, query)
        with self.source_lock:
            self.source_groups[key] = (time.monotonic(), [dict(item) for item in results[:50]
                                                        if len(item.get('uri', '')) <= 8192])
            self.source_groups.move_to_end(key)
            while len(self.source_groups) > 4:
                self.source_groups.popitem(last=False)

    def recovery_sources(self, title, query):
        with self.source_lock:
            entry = self.source_groups.get(self.source_key(title, query))
        if entry and time.monotonic() - entry[0] < 600:
            return entry[1]
        if query.get('tmdb') and query.get('type') == 'movie':
            payload, _ = self.catalog.search_payload('movie', query['tmdb'])
            method = 'search_movie'
        elif query.get('show') and query.get('episode') is not None:
            payload, _ = self.catalog.search_payload('tv', query['show'], query['season'], query['episode'])
            details = self.catalog.api('/tv/{}/season/{}/episode/{}'.format(query['show'], query['season'], query['episode']))
            if not details.get('id'):
                raise CatalogError('Episode details are unavailable')
            payload['tmdb_id'] = details['id']
            method = 'search_episode'
        else:
            return []
        results = self.search_providers(method, payload, timeout=15)
        self.remember_sources(title, query, results)
        return results

    def prepare_with_recovery(self, uri, selected, title, query):
        started = time.monotonic()
        self.last_recovery = {'attempts': [], 'alternative_used': False}
        peer_only = query.get('peer_only') == '1'
        def attempt(source, index):
            # Never retain tracker URLs or passkeys in the public diagnostics.
            identity = self.source_identity(source)
            safe_id = identity if re.fullmatch(r'[a-f0-9]{40}', identity) else 'torrent-url'
            record = {'info_hash': safe_id, 'status': 'preparing'}
            self.last_recovery['attempts'].append(record)
            attempt_started = time.monotonic()
            try:
                prepared = self.engine.prepare(source, index, title, peer_only=peer_only)
                self.source_health.record(source,True,time.monotonic()-attempt_started)
                record['status'] = 'ready'
                return prepared
            except (TorrentError, OSError) as error:
                if 'cancel' not in str(error).lower() and not self.kodi.abort_requested():
                    self.source_health.record(source,False)
                record.update(status='failed', error_type=type(error).__name__)
                self.last_failure = dict(getattr(self.engine, 'last_preparation', {}))
                raise
        try:
            return attempt(uri, selected), uri
        except (TorrentError, OSError) as original:
            if (self.kodi.setting('source_fallback') == 'false' or query.get('fallback') == '0'
                    or self.kodi.abort_requested() or self.shutdown.is_set() or 'cancel' in str(original).lower()):
                raise
            try:
                sources = self.recovery_sources(title, query)
            except (TorrentError, CatalogError, OSError, ValueError):
                raise original from None
            original_id = self.source_identity(uri)
            original_name = next((item.get('name', '') for item in sources
                                  if self.source_identity(item['uri']) == original_id), uri)
            maximum = resolution(original_name)
            attempted = {original_id}
            last_error = original
            configured_timeout = self.number(self.kodi.setting('buffer_timeout')) or 120
            per_attempt = max(30, min(180, configured_timeout - 15))
            fallback_window = min(90, max(0, configured_timeout * 2 - per_attempt - 30))
            for source in sources:
                identity = self.source_identity(source['uri'])
                if identity in attempted or (maximum and resolution(source.get('name', '')) > maximum):
                    continue
                # Leave room for one bounded engine attempt inside the Kodi resolver timeout.
                if len(attempted) >= 3 or time.monotonic() - started >= fallback_window:
                    break
                if self.kodi.abort_requested() or self.shutdown.is_set():
                    raise TorrentError('Cancelled')
                attempted.add(identity)
                self.kodi.notify('Source unavailable; trying another source for this title')
                try:
                    prepared = attempt(source['uri'], None)
                    self.last_recovery['alternative_used'] = True
                    return prepared, source['uri']
                except (TorrentError, OSError) as error:
                    if self.kodi.abort_requested() or 'cancel' in str(error).lower():
                        raise
                    last_error = error
            raise last_error from None

    def recovery_request(self, session, position):
        """Queue at most two same-title replacements after a confirmed stall."""
        if (self.kodi.setting('playback_recovery') == 'false' or self.shutdown.is_set()
                or self.kodi.abort_requested() or self.engine.active is not session
                or session.get('offline') or not 15 <= position <= 86400):
            return None
        query = session.get('playback_query') or {}
        if not (query.get('tmdb') and query.get('type') == 'movie' or
                query.get('show') and query.get('episode') is not None):
            return None  # A free-text magnet must never select a different film.
        chain_id = session.get('recovery_chain')
        chain = self.recovery_chains.get(chain_id)
        if not chain or chain['count'] >= 2 or time.monotonic()-chain['when'] > 1200:
            return None
        with self.source_lock:
            remembered = self.source_groups.get(self.source_key(session['title'],query))
            sources = list(remembered[1]) if remembered and time.monotonic()-remembered[0] < 1200 else []
        current = self.source_identity(session['uri'])
        maximum = resolution(next((source.get('name','') for source in sources
                                   if self.source_identity(source['uri']) == current), ''))
        for source in self.source_health.rank(sources):
            key = self.source_identity(source['uri'])
            if key in chain['attempted'] or key == current or (maximum and resolution(source.get('name')) > maximum):
                continue
            chain['attempted'].add(key)
            chain.update(count=chain['count']+1,position=float(position),expected=key)
            self.source_health.record(session['uri'],False)
            self.last_recovery['midstream'] = {'attempt':chain['count'],'resume_seconds':round(position,2)}
            return plugin_url('/play',uri=source['uri'],query=session['title'],resume='0',fallback='0',
                              _recovery=chain_id,peer_only='1' if session.get('peer_only') else '0',**query)
        return None

    def download_route(self, path, query):
        if path == '/downloads':
            jobs = self.downloads.snapshot()
            items = [list_item('Add magnet or torrent URL', plugin_url('/downloads/add'), action=True)]
            items += [list_item(job['title'], plugin_url('/downloads/' + job['id']),
                label2='{} | {:.1f}% | {:.1f} MiB/s'.format(job['status'], job['progress'], job['rate'] / 1048576))
                for job in reversed(jobs)]
            return view('files', items)
        if path == '/downloads/add':
            uri = query.get('uri') or self.kodi.input('Magnet link or .torrent URL')
            if uri:
                self.downloads.add(uri, query.get('query', ''),
                    self.number(query['oindex']) if 'oindex' in query else None,
                    all_files=query.get('all_files') == '1', peer_only=query.get('peer_only') == '1', media=query)
                self.kodi.notify('Added to the on-device download queue; keep Kodi open')
            return view('', [])
        match = re.fullmatch(r'/downloads/([a-f0-9]{24})(?:/(pause|resume|cancel|recheck|remove|play))?', path)
        if not match:
            raise TorrentError('Unknown download action')
        identity, action = match.groups()
        job = self.downloads.snapshot(identity)[0]
        if action in ('pause', 'resume', 'cancel', 'recheck'):
            self.downloads.control(identity, action)
            self.kodi.notify('Download ' + action + ' requested')
            return view('', [])
        if action == 'remove':
            if self.kodi.confirm('Delete this download and its saved data?'):
                self.remove_download(identity)
            return view('', [])
        if action == 'play':
            if not self.play_lock.acquire(blocking=False):
                raise TorrentError('Another video is being prepared')
            try:
                saved = self.downloads.playback(identity, self.number(query['oindex']) if 'oindex' in query else None)
                previous = next((item for item in self.state['history'] if item.get('download') == identity
                                 and item['file_index'] == saved['file_index']), None)
                self.pending_resume = previous.get('position', 0) if previous and self.kodi.resume(previous.get('position', 0)) else 0
                self.engine.stop()
                self.engine = TorrentEngine(saved['profile'], self.kodi.setting)
                prepared = self.engine.prepare_cached(saved['info_hash'], saved['file_index'], saved['title'])
                metadata = {'info': {'title': saved['title'], 'mediatype': 'video'}}
                try:
                    metadata = playback_metadata(self.catalog, saved['media']) or metadata
                except (CatalogError, ValueError):
                    pass
                self.last_info, self.playback_query = metadata, saved['media']
                self.engine.active.update(metadata=metadata, playback_query=saved['media'], download=identity)
                history = {'download': identity, 'uri': '', 'file_index': saved['file_index'], 'title': saved['title'],
                           'when': time.time(), 'position': self.pending_resume, 'duration': 0, 'media': saved['media']}
                self.state['history'] = [history] + [item for item in self.state['history']
                    if item.get('download') != identity or item['file_index'] != saved['file_index']][:99]
                self.save()
                return Redirect(prepared['url'])
            finally:
                self.play_lock.release()
        actions = [('Pause', 'pause'), ('Resume / retry', 'resume'), ('Cancel', 'cancel'),
                   ('Verify saved pieces', 'recheck'), ('Delete saved data', 'remove')]
        items = [list_item(label, plugin_url('/downloads/{}/{}'.format(identity, route)), action=True)
                 for label, route in actions]
        if job['status'] == 'completed':
            items[:0] = [list_item('Play ' + item['name'], plugin_url('/downloads/{}/play'.format(identity),
                          oindex=item['index']), playable=True) for item in job['files']
                          if item['index'] in job['selected_files'] and item['bytes'] > 0]
        if job.get('error'):
            items.append(list_item(job['error'], plugin_url('/downloads/' + identity)))
        return view('files', items)

    def remove_download(self, identity):
        if not self.play_lock.acquire(blocking=False):
            raise TorrentError('Wait for video preparation before deleting saved data')
        try:
            if self.engine.active and self.engine.active.get('download') == identity:
                raise TorrentError('Stop playing this saved download before deleting it')
            self.downloads.remove(identity, delete_data=True)
        finally:
            self.play_lock.release()

    def management_route(self, path, query):
        if path == '/playback/cache':
            self.kodi.optimize_cache()
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
        if path == '/integrations':
            return view('', [list_item(label + ' - install / configure',
                                      plugin_url('/integrations/' + name), action=True)
                             for name, (_, label) in COMPANIONS.items()])
        if path.startswith('/integrations/'):
            name = path.rsplit('/', 1)[-1]
            if name not in COMPANIONS:
                raise TorrentError('Unknown companion add-on')
            self.kodi.companion(COMPANIONS[name][0])
            return view('', [])
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

    def announce_next_episode(self, expected_url):
        session = self.engine.active
        if not session or session['stream'].url != expected_url or not self.kodi.has_addon('service.upnext'):
            return
        query = dict(session.get('playback_query', {}))
        payload = upnext_payload(self.catalog, query)
        # Catalog requests must never deliver a late notification for a different video.
        session = self.engine.active
        if payload and session and session['stream'].url == expected_url:
            self.kodi.rpc('JSONRPC.NotifyAll', upnext_notification(payload))


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
                    getattr(owner.backend.kodi, 'log_error', owner.backend.kodi.error)(type(error).__name__, str(error))
                    owner.backend.kodi.progress_close()
                    self.send_json({'error': type(error).__name__, 'message': str(error)}, 503)
                except Exception as error:
                    getattr(owner.backend.kodi, 'log_error', owner.backend.kodi.error)(type(error).__name__, 'Backend request failed')
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
        if self.backend.web:
            self.backend.web.close()
        self.backend.downloads.close()
        self.backend.engine.stop()
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=3)
