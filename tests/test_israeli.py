"""Catalog categories for leading and new Israeli series."""

import datetime
import json
from pathlib import Path
import sys
import tempfile
import unittest
from urllib.error import HTTPError
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'addon/resources/lib'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from elementum_py.backend import Backend, BackendServer
from elementum_py.catalog import Catalog
from test_backend import FakeEngine, FakeKodi


class IsraeliSeriesTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.requests = []
        self.pages = 3
        self.made = 0
        self.kodi = FakeKodi()
        self.backend = self.make()

    def make(self):
        self.made += 1                                   # Each backend gets its own catalog cache.
        backend = Backend(self.folder.name, self.kodi, port=0, engine=FakeEngine(),
                          catalog=Catalog('{}/catalog-{}'.format(self.folder.name, self.made), transport=self.transport))
        self.addCleanup(backend.close, 0)
        return backend

    def transport(self, path, params):
        self.requests.append((path, dict(params)))
        return {'total_pages': self.pages, 'results': [
            {'id': 71, 'name': 'Checkout', 'original_name': 'קופה ראשית', 'first_air_date': '2018-02-21'},
            {'id': 72, 'name': 'פאודה', 'original_name': 'פאודה', 'first_air_date': '2015-02-15'}]}

    def labels(self, result):
        return [item['label'] for item in result['items']]

    def test_series_menu_offers_both_lists_and_the_film_menu_does_not(self):
        shows = self.backend.catalog_list('/shows', {})
        self.assertEqual(self.labels(shows)[-3:], ['Top Israeli series', 'New Israeli series', 'Library'])
        paths = [item['path'] for item in shows['items']]
        self.assertIn('plugin://plugin.video.elementum/shows/israeli/top', paths)
        self.assertIn('plugin://plugin.video.elementum/shows/israeli/new', paths)
        films = self.backend.catalog_list('/movies', {})
        self.assertFalse([label for label in self.labels(films) if 'Israeli' in label])
        self.assertEqual(len(films['items']), len(shows['items']) - 2)      # Nothing else changed.

    def test_menu_names_follow_the_language_of_kodi(self):
        self.kodi.language = lambda: 'he-IL'
        shows = self.make().catalog_list('/shows', {})
        self.assertEqual(self.labels(shows)[-3:-1], ['סדרות ישראליות מובילות', 'סדרות ישראליות חדשות'])

    def test_leading_list_asks_for_israeli_series_by_number_of_votes(self):
        result = self.backend.catalog_list('/shows/israeli/top', {})
        path, params = self.requests[-1]
        self.assertEqual(path, '/discover/tv')
        self.assertEqual({key: params[key] for key in ('with_origin_country', 'sort_by', 'without_genres', 'page')},
                         {'with_origin_country': 'IL', 'sort_by': 'vote_count.desc', 'without_genres': '10763,10767', 'page': 1})
        self.assertNotIn('first_air_date.gte', params)
        self.assertEqual(result['content_type'], 'tvshows')
        self.assertTrue(result['items'][0]['path'].endswith('/show/71/seasons'))

    def test_new_list_asks_for_the_last_eighteen_months_by_popularity(self):
        before = datetime.date.today()
        self.backend.catalog_list('/shows/israeli/new', {})
        path, params = self.requests[-1]
        days = {before, datetime.date.today()}                                  # The same day, unless midnight passed.
        self.assertEqual(path, '/discover/tv')
        self.assertEqual((params['with_origin_country'], params['sort_by'], params['without_genres']),
                         ('IL', 'popularity.desc', '10763,10767'))
        self.assertIn(params['first_air_date.lte'], {day.isoformat() for day in days})   # Nothing that has not aired yet.
        self.assertIn(params['first_air_date.gte'], {(day - datetime.timedelta(days=548)).isoformat() for day in days})

    def test_translated_name_is_shown_with_the_hebrew_one(self):
        result = self.backend.catalog_list('/shows/israeli/top', {})
        self.assertEqual(self.labels(result)[:2], ['Checkout · קופה ראשית', 'פאודה'])
        # Kodi lists a video item by its title, so the title carries both names as well.
        self.assertEqual(result['items'][0]['info']['title'], 'Checkout · קופה ראשית')
        self.assertEqual(result['items'][0]['info']['originaltitle'], 'קופה ראשית')
        ordinary = self.backend.catalog_list('/shows/popular', {})
        self.assertEqual(self.labels(ordinary)[:2], ['Checkout', 'פאודה'])       # Other lists are as before.

    def test_next_page_stays_in_the_same_list(self):
        result = self.backend.catalog_list('/shows/israeli/new', {'page': '2'})
        self.assertEqual(self.requests[-1][1]['page'], 2)
        self.assertEqual(result['items'][-1]['path'], 'plugin://plugin.video.elementum/shows/israeli/new?page=3')
        self.pages = 2
        last = self.make().catalog_list('/shows/israeli/new', {'page': '2'})
        self.assertEqual(len(last['items']), 2)                                 # No link after the last page.

    def test_name_missing_in_the_language_of_kodi_shows_the_hebrew_one_alone(self):
        self.transport = lambda path, params: {'total_pages': 1, 'results': [
            {'id': 73, 'name': '', 'original_name': 'המעברה', 'first_air_date': '2026-03-09'}]}
        result = self.make().catalog_list('/shows/israeli/new', {})
        self.assertEqual(self.labels(result), ['המעברה'])

    def test_lists_are_served_over_the_local_api(self):
        server = BackendServer(self.backend)
        self.addCleanup(server.close)
        with urlopen(self.backend.base_url + '/shows/israeli/top', timeout=5) as response:
            listing = json.load(response)
        self.assertEqual(listing['content_type'], 'tvshows')
        self.assertEqual(listing['items'][0]['label'], 'Checkout · קופה ראשית')
        with self.assertRaises(HTTPError) as raised:
            urlopen(self.backend.base_url + '/movies/israeli/top', timeout=5)
        self.assertEqual(raised.exception.code, 501)
        raised.exception.close()

    def test_films_have_no_such_list(self):
        with self.assertRaises(NotImplementedError):
            self.backend.catalog_list('/movies/israeli/top', {})
        with self.assertRaises(NotImplementedError):
            self.backend.catalog_list('/shows/israeli/other', {})


if __name__ == '__main__':
    unittest.main()
