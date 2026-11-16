import json
from pathlib import Path
import sys
import tempfile
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'addon/resources/lib'))
from elementum_py.backend import Backend, BackendServer
from elementum_py.catalog import Catalog
from elementum_py.web import WebServer
from test_backend import FakeEngine, FakeKodi, catalog_transport


class WebTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.backend = Backend(self.folder.name, FakeKodi(), port=0, engine=FakeEngine(),
                               catalog=Catalog(self.folder.name + '/catalog', transport=catalog_transport))
        self.server = BackendServer(self.backend)
        self.web = WebServer(self.backend, port=0)
        self.backend.web = self.web

    def tearDown(self):
        self.server.close()
        self.folder.cleanup()

    def request(self, path, body=None, auth=True, headers=None):
        values = {'Authorization': 'Bearer ' + self.web.token} if auth else {}
        values.update(headers or {})
        if body is not None:
            values['Content-Type'] = 'application/json'
        return urlopen(Request(self.web.address.rstrip('/') + path,
                              data=json.dumps(body).encode() if body is not None else None, headers=values), timeout=3)

    def test_dashboard_assets_csp_no_secrets_and_auth_required(self):
        for path in ('/', '/app.js', '/style.css'):
            with self.request(path, auth=False) as response:
                self.assertEqual(response.status, 200)
                self.assertIn("frame-ancestors 'none'", response.headers['Content-Security-Policy'])
                self.assertNotIn(self.web.token.encode(), response.read())
        with self.assertRaises(HTTPError) as caught:
            self.request('/api/status', auth=False)
        self.assertEqual(caught.exception.code, 401)
        with self.request('/api/status') as response:
            self.assertEqual(json.load(response)['downloads'], [])

    def test_cross_origin_and_dns_rebinding_rejected(self):
        for headers in ({'Origin': 'https://attacker.invalid'}, {'Host': 'attacker.invalid:65222'},
                        {'Sec-Fetch-Site': 'cross-site'}, {'Origin': 'null'}):
            with self.assertRaises(HTTPError) as caught:
                self.request('/api/downloads', {'uri': 'magnet:?x=1'}, headers=headers)
            self.assertEqual(caught.exception.code, 403)
        self.assertEqual(self.backend.downloads.snapshot(), [])

    def test_add_pause_remove_contract_and_protected_delete(self):
        self.backend.downloads._parallelism = lambda: 0
        with self.request('/api/downloads', {'uri': 'magnet:?xt=urn:btih:' + 'a' * 40,
                                            'title': '<script>alert(1)</script>'}) as response:
            identity = json.load(response)['id']
        self.backend.downloads.control(identity, 'pause')
        with self.assertRaises(HTTPError):
            self.request('/api/downloads/' + identity + '/remove', {})
        with self.request('/api/status') as response:
            job = json.load(response)['downloads'][0]
            self.assertNotIn('uri', job)
            self.assertEqual(job['status'], 'paused')
        with self.request('/api/downloads/' + identity + '/remove', {'confirm_delete': True}) as response:
            self.assertEqual(response.status, 200)
        self.assertEqual(self.backend.downloads.snapshot(), [])

    def test_token_rotates_on_restart_and_get_cannot_mutate(self):
        old = self.web.token
        with self.assertRaises(HTTPError) as caught:
            self.request('/api/downloads/' + 'a' * 24 + '/cancel')
        self.assertEqual(caught.exception.code, 404)
        self.web.close()
        self.web = WebServer(self.backend, port=0)
        self.backend.web = self.web
        self.assertNotEqual(old, self.web.token)
        with self.assertRaises(HTTPError) as caught:
            self.request('/api/status', headers={'Authorization': 'Bearer ' + old})
        self.assertEqual(caught.exception.code, 401)


if __name__ == '__main__':
    unittest.main()
