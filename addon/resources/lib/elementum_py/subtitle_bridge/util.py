import gzip
import json
import os
import time
import zlib
import io
import ipaddress
import re
import secrets
import socket
from urllib.parse import urlsplit
from urllib.request import Request, HTTPRedirectHandler, build_opener

import xbmc
import xbmcaddon
import xbmcvfs

ADDON_ID = 'plugin.video.elementum'
UA = 'Mozilla/5.0 (Kodi; SubBridge)'
MAX_RESPONSE = 8 * 1024 * 1024


def addon():
    return xbmcaddon.Addon(ADDON_ID)


def log(msg, level=xbmc.LOGINFO):
    msg = re.sub(r'https?://\S+', '[subtitle source]', str(msg))
    xbmc.log('[Elementum subtitles] %s' % msg, level)


def setting(key, default=''):
    try:
        return addon().getSetting('subs_' + key)
    except Exception:
        return default


def setting_bool(key, default=False):
    try:
        return addon().getSettingBool('subs_' + key)
    except Exception:
        return default


def setting_int(key, default=0):
    try:
        return addon().getSettingInt('subs_' + key)
    except Exception:
        return default


def profile_dir():
    path = os.path.join(xbmcvfs.translatePath(addon().getAddonInfo('profile')), 'subtitles')
    os.makedirs(path, exist_ok=True)
    return path


def temp_dir():
    path = os.path.join(profile_dir(), 'files')
    os.makedirs(path, exist_ok=True)
    return path


def validate_url(url):
    if not isinstance(url, str) or len(url) > 8192 or any(c in url for c in '\r\n\0'):
        raise ValueError('Invalid subtitle URL')
    parsed = urlsplit(url)
    if parsed.scheme not in ('https', 'http') or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError('Use a public HTTP(S) subtitle URL')
    if parsed.port not in (None, 80, 443):
        raise ValueError('Unsupported subtitle server port')
    answers = socket.getaddrinfo(parsed.hostname, parsed.port or (443 if parsed.scheme == 'https' else 80), type=socket.SOCK_STREAM)
    if not answers or any(not ipaddress.ip_address(item[4][0].split('%')[0]).is_global for item in answers):
        raise ValueError('Subtitle source is not a public server')
    return url


class SafeRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        validate_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def get(url, timeout, accept='*/*'):
    """Bound network and decompressed bytes; retain standard TLS verification."""
    validate_url(url)
    req = Request(url, headers={'User-Agent': UA, 'Accept': accept, 'Accept-Encoding': 'gzip, deflate'})
    with build_opener(SafeRedirect()).open(req, timeout=max(3, min(45, timeout))) as r:
        data = r.read(MAX_RESPONSE + 1)
        enc = (r.headers.get('Content-Encoding') or '').lower()
    if len(data) > MAX_RESPONSE:
        raise ValueError('Subtitle response is too large')
    if enc == 'gzip' or data[:2] == b'\x1f\x8b':
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as compressed:
            data = compressed.read(MAX_RESPONSE + 1)
    elif enc == 'deflate':
        try:
            data = zlib.decompressobj().decompress(data, MAX_RESPONSE + 1)
        except zlib.error:
            data = zlib.decompressobj(-zlib.MAX_WBITS).decompress(data, MAX_RESPONSE + 1)
    if len(data) > MAX_RESPONSE:
        raise ValueError('Decompressed subtitle response is too large')
    return data


def get_json(url, timeout):
    value = json.loads(get(url, timeout, 'application/json').decode('utf-8'))
    if not isinstance(value, dict):
        raise ValueError('Invalid subtitle catalog response')
    return value


def rpc(method, params=None):
    q = {'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params or {}}
    try:
        return json.loads(xbmc.executeJSONRPC(json.dumps(q))).get('result') or {}
    except Exception:
        return {}


class Store:
    """A small JSON file in the add-on's profile folder, entries kept for max_age seconds."""

    def __init__(self, name, max_age):
        self.path = os.path.join(profile_dir(), name)
        self.max_age = max_age
        try:
            with open(self.path, encoding='utf-8') as fh:
                self.data = json.loads(fh.read(MAX_RESPONSE + 1))
            if not isinstance(self.data, dict):
                self.data = {}
        except Exception:
            self.data = {}
        now = time.time()
        self.data = {k: v for k, v in self.data.items()
                     if isinstance(v, dict) and isinstance(v.get('t'), (int, float)) and now - v['t'] < max_age}

    def get(self, key):
        v = self.data.get(key)
        return v.get('v') if v else None

    def put(self, key, value):
        self.data[key] = {'t': time.time(), 'v': value}

    def save(self):
        tmp = self.path + '.' + secrets.token_hex(8) + '.tmp'
        try:
            selected = sorted(self.data.items(), key=lambda pair: pair[1]['t'], reverse=True)[:20]
            data = json.dumps(dict(selected)).encode('utf-8')
            while len(data) > MAX_RESPONSE and selected:
                selected.pop()
                data = json.dumps(dict(selected)).encode('utf-8')
            descriptor = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, 'wb') as fh:
                fh.write(data)
            os.replace(tmp, self.path)
        except Exception as e:
            log('could not save %s: %s' % (self.path, e), xbmc.LOGWARNING)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

