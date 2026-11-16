import base64
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'addon/resources/lib'))
from elementum_py.backend import Backend, BackendServer, Redirect
from elementum_py.catalog import Catalog


class FakeKodi:
    def __init__(self):
        self.backend = None
        self.messages = []
        self.last_payload = None

    def setting(self, key):
        return ''

    def language(self):
        return 'en-US'

    def addons(self):
        return [{'addonid': 'script.elementum.burst', 'name': 'Burst', 'enabled': True},
                {'addonid': 'script.ios.elementum.enginecheck', 'name': 'Check', 'enabled': True}]

    def execute_addon(self, addon_id, params):
        self.last_payload = json.loads(base64.b64decode(params[0]))
        callback_url = self.last_payload['callback_url']
        result = [{'name': 'Fixture film', 'uri': 'magnet:?xt=urn:btih:' + 'a' * 40,
                   'info_hash': 'a' * 40, 'seeds': 20, 'size': '200 MB', 'provider': 'Fixture'}]
        with urlopen(Request(callback_url, data=json.dumps(result).encode('utf-8')), timeout=3) as response:
            assert response.status == 200

    def input(self, prompt):
        return 'Sintel'

    def choose_file(self, names):
        return 0

    def abort_requested(self):
        return False

    def notify(self, message):
        self.messages.append(message)

    def progress(self, message):
        pass

    def progress_close(self):
        pass

    def error(self, kind, message):
        self.messages.append(kind)

    def resume(self, position):
        return False

    def open_settings(self, addon_id):
        self.messages.append(addon_id)


class FakeEngine:
    active = None

    def __init__(self):
        self.prepared = None

    def stop(self):
        pass

    def stats(self):
        return {'active': False}

    def prepare(self, uri, selected, title, peer_only=False):
        self.prepared = {'uri': uri, 'title': title, 'peer_only': peer_only}
        return {'title': title or 'Fixture', 'file_index': 0, 'url': 'http://127.0.0.1:12345/token/video.mp4'}


def catalog_transport(path, params):
    movie = {'id': 81747, 'title': 'Sintel', 'original_title': 'Sintel',
             'release_date': '2010-09-30', 'imdb_id': 'tt1727587', 'overview': 'CC test film', 'genres': []}
    if path == '/movie/81747':
        return movie
    if path == '/tv/1':
        return {'id': 1, 'name': 'Fixture show', 'original_name': 'Fixture show',
                'first_air_date': '2020-01-01', 'seasons': [{'season_number': 1, 'name': 'Season 1'}]}
    if path == '/tv/1/season/1':
        return {'episodes': [{'id': 11, 'episode_number': 2, 'name': 'Second episode'}]}
    if path == '/tv/1/season/1/episode/2':
        return {'id': 11, 'episode_number': 2, 'name': 'Second episode', 'air_date': '2020-01-08'}
    return {'results': [movie], 'total_pages': 2}


class BackendTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.kodi = FakeKodi()
        self.engine = FakeEngine()
        self.catalog = Catalog(self.folder.name + '/catalog', transport=catalog_transport)
        self.backend = Backend(self.folder.name, self.kodi, port=0, catalog=self.catalog, engine=self.engine)
        self.server = BackendServer(self.backend)

    def tearDown(self):
        self.server.close()
        self.folder.cleanup()

    def get(self, path):
        with urlopen(self.backend.base_url + path, timeout=4) as response:
            return json.load(response)

    def test_original_frontend_view_contract(self):
        root = self.get('/')
        self.assertIn('content_type', root)
        self.assertGreater(len(root['items']), 5)
        for item in root['items']:
            self.assertTrue(all(key in item for key in ('label', 'label2', 'icon', 'thumbnail', 'is_playable', 'path')))

    def test_real_callback_burst_payload_contract(self):
        result = self.get('/search?q=Sintel')
        self.assertEqual(len(result['items']), 1)
        self.assertTrue(result['items'][0]['is_playable'])
        self.assertEqual(self.kodi.last_payload['method'], 'search')
        self.assertEqual(self.kodi.last_payload['search_object']['query'], 'Sintel')
        self.assertEqual(self.backend.callbacks, {})

    def test_diagnostic_not_misidentified_as_provider(self):
        self.assertEqual(len(self.backend.providers()), 1)

    def test_movie_search_object_and_info(self):
        self.get('/movie/81747/links')
        payload = self.kodi.last_payload
        self.assertEqual(payload['method'], 'search_movie')
        self.assertEqual(payload['search_object']['imdb_id'], 'tt1727587')
        self.assertEqual(self.get('/movie/81747/infolabels')['info']['title'], 'Sintel')

    def test_catalog_pagination(self):
        result = self.get('/movies/popular')
        self.assertEqual(len(result['items']), 2)
        self.assertIn('page=2', result['items'][-1]['path'])

    def test_seasons_episodes_and_episode_payload(self):
        self.assertIn('season/1/episodes', self.get('/show/1/seasons')['items'][0]['path'])
        self.assertIn('episode/2/links', self.get('/show/1/season/1/episodes')['items'][0]['path'])
        self.get('/show/1/season/1/episode/2/links')
        self.assertEqual(self.kodi.last_payload['search_object']['tmdb_id'], 11)
        self.assertEqual(self.kodi.last_payload['search_object']['show_tmdb_id'], 1)

    def test_unknown_callback_rejected(self):
        request = Request(self.backend.base_url + '/callbacks/' + '0' * 48, data=b'[]')
        with self.assertRaises(HTTPError) as caught:
            urlopen(request, timeout=3)
        self.assertEqual(caught.exception.code, 410)
        caught.exception.close()

    def test_unknown_endpoint_does_not_claim_success(self):
        with self.assertRaises(HTTPError) as caught:
            self.get('/unported-feature')
        self.assertEqual(caught.exception.code, 501)
        caught.exception.close()

    def test_playback_redirect_and_history(self):
        result = self.backend.route('/play', {'uri': 'magnet:?xt=urn:btih:' + 'a' * 40, 'query': 'Fixture'})
        self.assertIsInstance(result, Redirect)
        self.assertEqual(result.status, 302)
        self.assertEqual(self.backend.state['history'][0]['title'], 'Fixture')
        self.assertEqual(len(self.get('/history')['items']), 1)

    def test_library_addition_creates_playable_stream_file(self):
        self.get('/library/movie/add/81747')
        result = self.get('/movies/library')
        self.assertEqual(result['items'][0]['info']['title'], 'Sintel')
        text = Path(self.folder.name, 'library/movie/81747.strm').read_text(encoding='utf-8')
        self.assertIn('/movie/81747/play', text)


if __name__ == '__main__':
    unittest.main(verbosity=2)
