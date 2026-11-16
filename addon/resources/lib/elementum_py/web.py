"""Token-protected, optional dashboard; never expose the internal Kodi API."""

import hmac
import json
import os
import re
import secrets
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from .catalog import plugin_url
from .core import TorrentError
from .version import VERSION


class WebServer:
    def __init__(self, backend, port=65222, lan=False):
        self.backend, self.lan = backend, lan
        self.token = secrets.token_urlsafe(32)  # Rotates whenever the service restarts.
        owner = self
        assets = os.path.join(os.path.dirname(__file__), 'web')

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def setup(self):
                super().setup()
                self.connection.settimeout(10)

            def log_message(self, *args):
                pass

            def send(self, body, status=200, content_type='application/json; charset=utf-8'):
                if not isinstance(body, bytes):
                    body = json.dumps(body, ensure_ascii=False).encode('utf-8')
                self.send_response(status)
                for key, value in {'Content-Type': content_type, 'Content-Length': str(len(body)),
                    'Cache-Control': 'no-store', 'Connection': 'close', 'X-Content-Type-Options': 'nosniff',
                    'Referrer-Policy': 'no-referrer', 'X-Frame-Options': 'DENY',
                    'Content-Security-Policy': "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"}.items():
                    self.send_header(key, value)
                self.end_headers()
                self.close_connection = True
                self.wfile.write(body)

            def allowed(self):
                host = self.headers.get('Host', '')
                try:
                    parsed = urlsplit('http://' + host)
                    address = parsed.hostname
                    if parsed.port != owner.httpd.server_port:
                        return False
                    if address not in ('127.0.0.1', 'localhost', '::1'):
                        # IP literals only: reject DNS rebinding and attacker-controlled names.
                        import ipaddress
                        ipaddress.ip_address(address)
                        if not owner.lan:
                            return False
                    origin = self.headers.get('Origin')
                    if origin and origin != 'http://' + host:
                        return False
                    if self.headers.get('Sec-Fetch-Site') == 'cross-site':
                        return False
                    return True
                except (ValueError, TypeError):
                    return False

            def authenticated(self):
                return hmac.compare_digest(self.headers.get('Authorization', '').encode('utf-8'),
                                           ('Bearer ' + owner.token).encode('ascii'))

            def do_GET(self):
                if not self.allowed():
                    self.send({'error': 'Untrusted origin or host'}, 403)
                    return
                path = urlsplit(self.path).path
                if path in ('/', '/app.js', '/style.css'):
                    name, mime = {'/': ('index.html', 'text/html; charset=utf-8'),
                                  '/app.js': ('app.js', 'text/javascript; charset=utf-8'),
                                  '/style.css': ('style.css', 'text/css; charset=utf-8')}[path]
                    with open(os.path.join(assets, name), 'rb') as handle:
                        self.send(handle.read(), content_type=mime)
                elif not self.authenticated():
                    self.send({'error': 'Access token required'}, 401)
                elif path == '/api/status':
                    self.send({'version': VERSION, 'downloads': owner.backend.downloads.snapshot(),
                               'player': owner.backend.engine.stats(), 'foreground_required': True})
                else:
                    self.send({'error': 'Not found'}, 404)

            def do_POST(self):
                if not self.allowed():
                    self.send({'error': 'Untrusted origin or host'}, 403)
                    return
                if not self.authenticated():
                    self.send({'error': 'Access token required'}, 401)
                    return
                try:
                    length = int(self.headers.get('Content-Length', '0'))
                    if not 0 < length <= 70000 or self.headers.get_content_type() != 'application/json':
                        raise ValueError('Expected a bounded JSON request')
                    payload = json.loads(self.rfile.read(length))
                    if not isinstance(payload, dict):
                        raise ValueError('Expected a JSON object')
                    path = urlsplit(self.path).path
                    if path == '/api/downloads':
                        identity = owner.backend.downloads.add(payload.get('uri'), payload.get('title', ''),
                            payload.get('file_index'), all_files=payload.get('all_files') is True)
                        self.send({'id': identity}, 201)
                        return
                    match = re.fullmatch(r'/api/downloads/([a-f0-9]{24})/(pause|resume|cancel|recheck|remove|play)', path)
                    if not match:
                        self.send({'error': 'Not found'}, 404)
                        return
                    identity, action = match.groups()
                    if action == 'remove':
                        if payload.get('confirm_delete') is not True:
                            raise ValueError('Confirm deletion of saved data')
                        owner.backend.remove_download(identity)
                    elif action == 'play':
                        owner.backend.downloads.playback(identity, payload.get('file_index'))
                        owner.backend.kodi.rpc('Player.Open', {'item': {'file': plugin_url(
                            '/downloads/{}/play'.format(identity), **({'oindex': payload['file_index']}
                            if payload.get('file_index') is not None else {}))}})
                    else:
                        owner.backend.downloads.control(identity, action)
                    self.send({'status': 'ok'})
                except (ValueError, TypeError, TorrentError, OSError) as error:
                    self.send({'error': re.sub(r'https?://\S+', '[source]', str(error))[:250]}, 400)
                except Exception:
                    self.send({'error': 'The requested operation failed'}, 500)

        class Server(ThreadingHTTPServer):
            daemon_threads = True

            def __init__(self, *args):
                self.slots = threading.BoundedSemaphore(12)
                super().__init__(*args)

            def process_request(self, request, client_address):
                if not self.slots.acquire(blocking=False):
                    self.shutdown_request(request)
                    return
                try:
                    super().process_request(request, client_address)
                except Exception:
                    self.slots.release()
                    raise

            def process_request_thread(self, request, client_address):
                try:
                    super().process_request_thread(request, client_address)
                finally:
                    self.slots.release()

        self.httpd = Server(('0.0.0.0' if lan else '127.0.0.1', port), Handler)
        address = '127.0.0.1'
        if lan:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as channel:
                    channel.connect(('192.0.2.1', 9))  # Route lookup only; no packets are sent.
                    address = channel.getsockname()[0]
            except OSError:
                pass
        self.address = 'http://{}:{}/'.format(address, self.httpd.server_port)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=3)
