import base64
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'addon/resources/lib'))
from elementum_py.catalog import Catalog
try:
    from elementum_py.integrations import next_episode, playback_metadata, upnext_notification, upnext_payload
except ImportError:
    raise unittest.SkipTest('Companion integrations belong to the development preview')


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.seasons = {
            1: [{'id': 101, 'episode_number': 1, 'name': 'First', 'air_date': '2020-01-01'},
                {'id': 102, 'episode_number': 2, 'name': 'Second', 'air_date': '2020-01-08'}],
            2: [{'id': 201, 'episode_number': 1, 'name': 'Return', 'air_date': '2021-01-01'},
                {'id': 202, 'episode_number': 2, 'name': 'Future', 'air_date': '2099-01-01'}]}
        self.show = {'id': 10, 'name': 'Series', 'external_ids': {'imdb_id': 'tt1234567', 'tvdb_id': 20},
                     'seasons': [{'season_number': 0}, {'season_number': 1}, {'season_number': 2}]}

        def api(path, params):
            if path == '/tv/10':
                return self.show
            if path == '/movie/30':
                return {'id': 30, 'title': 'Movie', 'imdb_id': 'tt7654321'}
            parts = path.split('/')
            season = int(parts[4])
            if len(parts) == 5:
                return {'episodes': self.seasons[season]}
            return next(episode for episode in self.seasons[season] if episode['episode_number'] == int(parts[6]))

        self.catalog = Catalog(self.directory.name, transport=api)

    def tearDown(self):
        self.directory.cleanup()

    def test_movie_ids_match_both_existing_scrobblers(self):
        metadata = playback_metadata(self.catalog, {'tmdb': '30', 'type': 'movie'})
        self.assertEqual(metadata['uniqueids'], {'tmdb': '30', 'imdb': 'tt7654321'})
        self.assertEqual(json.loads(metadata['properties']['script.trakt.ids']), metadata['uniqueids'])
        self.assertEqual(metadata['properties']['script.simkl.ids'], metadata['properties']['script.trakt.ids'])

    def test_episode_and_show_ids_never_confused(self):
        metadata = playback_metadata(self.catalog, {'show': '10', 'season': '1', 'episode': '2'})
        self.assertEqual(metadata['uniqueids']['tmdb'], '102')
        self.assertEqual(json.loads(metadata['properties']['script.trakt.ids'])['tmdb'], '10')
        self.assertEqual(metadata['properties']['tvshow.tvdb_id'], '20')
        self.assertEqual(metadata['info']['mediatype'], 'episode')

    def test_next_episode_same_season(self):
        self.assertEqual(next_episode(self.catalog, 10, 1, 1, today='2025-01-01')[1]['id'], 102)

    def test_next_episode_season_transition(self):
        following = next_episode(self.catalog, 10, 1, 2, today='2025-01-01')
        self.assertEqual((following[0], following[1]['id']), (2, 201))

    def test_future_and_unknown_airdate_not_autoplayed(self):
        self.assertIsNone(next_episode(self.catalog, 10, 2, 1, today='2025-01-01'))
        self.seasons[2][1]['air_date'] = None
        self.catalog = Catalog(str(Path(self.directory.name) / 'changed'), transport=self.catalog.transport)
        self.assertIsNone(next_episode(self.catalog, 10, 2, 1, today='9999-01-01'))

    def test_upnext_official_notification_contract(self):
        payload = upnext_payload(self.catalog, {'show': '10', 'season': '1', 'episode': '2'})
        self.assertEqual(payload['next_episode']['episodeid'], 'tmdb:201')
        self.assertTrue(payload['play_url'].endswith('/show/10/season/2/episode/1/autoplay'))
        self.assertEqual(set(payload['current_episode']), {'episodeid', 'tvshowid', 'title', 'season', 'episode',
                                                          'showtitle', 'plot', 'playcount', 'rating', 'firstaired', 'runtime', 'art'})
        message = upnext_notification(payload)
        self.assertEqual(message['sender'], 'plugin.video.elementum.SIGNAL')
        self.assertEqual(message['message'], 'upnext_data')
        self.assertEqual(json.loads(base64.b64decode(message['data'][0])), payload)

    def test_generic_magnet_not_guessed_as_movie_or_episode(self):
        self.assertIsNone(playback_metadata(self.catalog, {'uri': 'magnet:?xt=urn:btih:' + 'a' * 40}))
        self.assertIsNone(upnext_payload(self.catalog, {'tmdb': '30', 'type': 'movie'}))


if __name__ == '__main__':
    unittest.main()
