"""Native subtitle rows, release identity, Hebrew encoding and bounded downloads."""
import gzip
import io
import json
from pathlib import Path
import socket
import struct
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'addon/resources/lib'))
from elementum_py.core import Metainfo, PieceStore, bencode
from elementum_py.subtitle_context import build_context
from elementum_py.subtitle_dispatch import is_subtitle_request

state = {'settings': {}, 'rows': [], 'path': '', 'context': '', 'profile': ''}
def language(text, target):
    code = {'he': 'heb', 'heb': 'heb', 'iw': 'heb', 'hebrew': 'heb', 'en': 'eng', 'eng': 'eng',
            'english': 'eng', 'ar': 'ara', 'ara': 'ara', 'arabic': 'ara'}.get(str(text).lower(), '')
    if target == 1:
        return {'heb': 'he', 'eng': 'en', 'ara': 'ar'}.get(code, '')
    if target == 3:
        return {'heb': 'Hebrew', 'eng': 'English', 'ara': 'Arabic'}.get(code, '')
    return code

class Addon:
    def __init__(self, *args): pass
    def getSetting(self, key): return state['settings'].get(key, '')
    def getSettingBool(self, key): return state['settings'].get(key, 'false') == 'true'
    def getSettingInt(self, key): return int(state['settings'].get(key, '0'))
    def getAddonInfo(self, key): return state['profile'] if key == 'profile' else 'plugin.video.elementum'

class Item:
    def __init__(self, label='', label2='', **kwargs): self.label, self.label2, self.props, self.art = label, label2, {}, {}
    def setArt(self, values): self.art.update(values)
    def setProperty(self, key, value): self.props[key] = value

class Player:
    def getPlayingFile(self): return state['path']
    def getVideoInfoTag(self): return None

stubs = {
    'xbmc': types.SimpleNamespace(LOGINFO=1, LOGWARNING=2, LOGERROR=3, ISO_639_1=1, ISO_639_2=2, ENGLISH_NAME=3,
        log=lambda *args: None, convertLanguage=language, Player=Player, getInfoLabel=lambda key: '', executeJSONRPC=lambda q:'{}'),
    'xbmcaddon': types.SimpleNamespace(Addon=Addon),
    'xbmcvfs': types.SimpleNamespace(translatePath=lambda path: path),
    'xbmcgui': types.SimpleNamespace(ListItem=Item, NOTIFICATION_WARNING='warning',
        Window=lambda *args: types.SimpleNamespace(getProperty=lambda key: state['context']),
        Dialog=lambda: types.SimpleNamespace(notification=lambda *args: None)),
    'xbmcplugin': types.SimpleNamespace(addDirectoryItems=lambda handle, items, length: state['rows'].extend(items),
        addDirectoryItem=lambda **kw: state['rows'].append(kw), endOfDirectory=lambda handle: None),
}
with patch.dict(sys.modules, stubs):
    from elementum_py.subtitle_bridge import service, stremio, util, video, langs


class SubtitleTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        state.update(profile=self.folder.name, rows=[], path='', context='', settings={
            'subs_prefer_hebrew': 'true', 'subs_opensubtitles': 'true', 'subs_hash': 'true', 'subs_timeout': '10'})
        self.text = '1\n00:00:01,000 --> 00:00:04,000\nשלום, Kodi! (2026)\n'

    def tearDown(self): self.folder.cleanup()

    def test_native_subtitle_actions_do_not_open_movie_directory(self):
        for action in ('search', 'manualsearch', 'download'):
            self.assertTrue(is_subtitle_request(['plugin://plugin.video.elementum/', '7', '?action=' + action]))
        self.assertFalse(is_subtitle_request(['plugin://plugin.video.elementum/search', '7', '?action=search']))
        self.assertFalse(is_subtitle_request(['plugin://plugin.video.elementum/', '7', '']))

    def test_language_aliases_and_hebrew_priority(self):
        self.assertEqual(langs.code('iw')[0], 'heb')
        self.assertEqual(service.wanted_languages({'languages': 'English', 'preferredlanguage': 'English'}), ['heb', 'eng'])
        state['settings']['subs_prefer_hebrew'] = 'false'
        self.assertEqual(service.wanted_languages({'languages': 'English'}), ['eng'])

    def test_actual_subtitle_rows_replace_movies_and_tv_menus(self):
        answer = [('https://example.org', 'Fixture subtitles', [
            {'id': 'he', 'lang': 'heb', 'url': 'https://example.org/sub.srt', 'label': 'Fixture.2026.1080p'},
            {'id': 'ar', 'lang': 'ara', 'url': 'https://example.org/ar.srt'}])]
        with patch.object(video, 'playing', return_value={'path': '', 'kind': 'movie', 'ids': ['tt1234567']}), \
                patch.object(video, 'file_details', return_value={'filename': 'Fixture.2026.1080p.mkv'}), \
                patch.object(service, 'collect', return_value=answer):
            service.do_search(7, {'languages': 'Hebrew'})
        self.assertEqual(len(state['rows']), 1)
        url, item, folder = state['rows'][0]
        self.assertEqual(item.label, 'Hebrew')
        self.assertEqual(item.art['thumb'], 'he')
        self.assertIn('action=download', url)
        self.assertIn('Fixture', item.label2)
        self.assertFalse(folder)
        self.assertEqual(item.props['sync'], 'true')

    def test_utf8_hebrew_is_not_reversed(self):
        raw, ext = service._subtitle_bytes(self.text.encode(), 'https://example.org/a.srt', 'heb')
        self.assertEqual(raw.decode(), self.text)
        self.assertEqual(ext, 'srt')

    def test_legacy_hebrew_cp1255_becomes_utf8(self):
        raw, _ = service._subtitle_bytes(self.text.encode('cp1255'), 'https://example.org/a.srt', 'heb')
        self.assertEqual(raw.decode(), self.text)

    def test_utf16_and_utf32_become_utf8(self):
        for encoding in ('utf-16', 'utf-32'):
            raw, _ = service._subtitle_bytes(self.text.encode(encoding), 'https://example.org/a.srt', 'heb')
            self.assertEqual(raw.decode(), self.text)

    def test_html_error_is_not_a_subtitle(self):
        with self.assertRaises(ValueError):
            service._subtitle_bytes(b'<html><!-- cached error -->login</html>', 'https://example.org/a.srt')

    def test_zip_is_read_without_extracting_its_path(self):
        body = io.BytesIO()
        with zipfile.ZipFile(body, 'w') as zipped:
            zipped.writestr('../../outside.srt', self.text)
        raw, ext = service._subtitle_bytes(body.getvalue(), 'https://example.org/archive.zip', 'heb')
        self.assertEqual(raw.decode(), self.text)
        self.assertEqual(ext, 'srt')
        self.assertEqual(list(Path(self.folder.name).iterdir()), [])

    def test_oversized_compressed_subtitle_rejected(self):
        body = io.BytesIO()
        with zipfile.ZipFile(body, 'w', zipfile.ZIP_DEFLATED) as zipped:
            zipped.writestr('big.srt', b'x' * (util.MAX_RESPONSE + 1))
        with self.assertRaises(ValueError):
            service._subtitle_bytes(body.getvalue(), 'https://example.org/archive.zip')

    def test_http_and_redirects_reject_private_addresses(self):
        private = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 443))]
        with patch.object(util.socket, 'getaddrinfo', return_value=private):
            with self.assertRaises(ValueError): util.validate_url('https://example.org/sub.srt')
        for url in ('file:///etc/passwd', 'https://user:password@example.org/a', 'https://example.org:65220/a'):
            with self.assertRaises(ValueError): util.validate_url(url)

    def test_gzip_download_has_decompressed_size_limit(self):
        class Response:
            headers = {'Content-Encoding': 'gzip'}
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, size): return gzip.compress(b'x' * (util.MAX_RESPONSE + 1))
        with patch.object(util, 'validate_url'), patch.object(util, 'build_opener', return_value=types.SimpleNamespace(open=lambda *a, **k: Response())):
            with self.assertRaises(ValueError): util.get('https://example.org/a', 3)

    def test_cache_atomic_writes_remain_valid(self):
        def save(index):
            cache = util.Store('searches.json', 30)
            cache.put(str(index), {'value': index})
            cache.save()
        threads = [threading.Thread(target=save, args=(index,)) for index in range(12)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(2)
        saved = json.loads(Path(util.profile_dir(), 'searches.json').read_text())
        self.assertTrue(saved)
        self.assertFalse(list(Path(util.profile_dir()).glob('*.tmp')))

    def test_episode_uses_show_id_and_original_release(self):
        session = {'metadata': {'info': {'mediatype': 'episode', 'tvshowtitle': 'Fixture show', 'season': 1, 'episode': 2},
                               'uniqueids': {'imdb': 'tt9999999'},
                               'properties': {'script.trakt.ids': json.dumps({'imdb': 'tt1111111'})}},
                   'playback_query': {'show': '1', 'season': '1', 'episode': '2'}}
        context = build_context(session, 'http://127.0.0.1/token/video.mkv')
        self.assertEqual(context['ids'], ['tt1111111'])
        self.assertEqual(video.stremio_id(context, context['ids'][0]), 'tt1111111:1:2')

    def test_context_applies_only_to_the_exact_playing_url(self):
        state['path'] = 'http://127.0.0.1/token/video.mkv'
        expected = {'path': state['path'], 'kind': 'movie', 'ids': ['tt1727587'], 'title': 'Sintel', 'year': 2010}
        state['context'] = json.dumps(expected)
        self.assertEqual(video.playing()['ids'], ['tt1727587'])
        state['path'] = 'http://127.0.0.1/different/video.mkv'
        self.assertNotEqual(video.playing().get('ids'), ['tt1727587'])

    def test_video_hash_uses_already_cached_bytes_without_fetching(self):
        import hashlib
        payload = bytes(range(256)) * 1024
        meta = Metainfo(bencode({b'info': {b'name': b'Fixture.2026.mkv', b'length': len(payload), b'piece length': 65536,
            b'pieces': b''.join(hashlib.sha1(payload[n:n+65536]).digest() for n in range(0,len(payload),65536))}}))
        store = PieceStore(meta, allow_webseeds=False)
        store.cache[0], store.cache[3] = payload[:65536], payload[-65536:]
        session = {'meta': meta, 'store': store, 'file_index': 0, 'metadata': {'uniqueids': {'imdb': 'tt1727587'}}}
        context = build_context(session, 'http://127.0.0.1/token/video.mkv')
        expected = len(payload) + sum(struct.unpack('<8192Q', payload[:65536])) + sum(struct.unpack('<8192Q', payload[-65536:]))
        self.assertEqual(context['videoHash'], '{:016x}'.format(expected & 0xffffffffffffffff))
        self.assertEqual(context['release'], 'Fixture.2026.mkv')
        self.assertEqual(store.verified_pieces, 0)  # No additional reads or transfers.
        store.close()


if __name__ == '__main__': unittest.main()
