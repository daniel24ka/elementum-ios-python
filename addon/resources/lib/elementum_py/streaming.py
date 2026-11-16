"""Loopback-only HTTP range streaming for the experimental Python engine."""

import hmac
import secrets
import threading
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .core import TorrentError


def parse_range(value, size):
    if size <= 0:
        raise TorrentError('Cannot stream an empty file')
    if value is None:
        return 0, size - 1, 200
    if not value.startswith('bytes=') or ',' in value:
        raise TorrentError('Only a single byte range is supported')
    limits = value[6:].split('-')
    if len(limits) != 2:
        raise TorrentError('Invalid byte range')
    start, end = limits
    if not start:
        if not end.isdigit() or int(end) <= 0:
            raise TorrentError('Invalid suffix byte range')
        return max(0, size - int(end)), size - 1, 206
    if not start.isdigit() or (end and not end.isdigit()):
        raise TorrentError('Invalid byte range')
    first = int(start)
    last = min(int(end), size - 1) if end else size - 1
    if first >= size or first > last:
        raise TorrentError('Unsatisfiable byte range')
    return first, last, 206


class StreamServer:
    def __init__(self, store, file_index):
        self.store = store
        self.file_index = file_index
        self.token = secrets.token_hex(24)
        extension = os.path.splitext(store.meta.files[file_index]['path'][-1])[1].lower()
        types = {'.mp4': 'video/mp4', '.m4v': 'video/mp4', '.mkv': 'video/x-matroska',
                 '.avi': 'video/x-msvideo', '.webm': 'video/webm', '.mov': 'video/quicktime', '.ts': 'video/mp2t'}
        self.extension = extension if extension in types else '.mp4'
        self.mime_type = types.get(extension, 'application/octet-stream')
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def do_HEAD(self):
                self.serve(False)

            def do_GET(self):
                self.serve(True)

            def serve(self, send_body):
                expected_path = '/' + owner.token + '/video' + owner.extension
                if not self.path.isascii() or not hmac.compare_digest(self.path, expected_path):
                    self.send_error(404)
                    return
                size = owner.store.meta.files[owner.file_index]['length']
                try:
                    start, end, status = parse_range(self.headers.get('Range'), size)
                except (TorrentError, ValueError):
                    self.send_response(416)
                    self.send_header('Content-Range', 'bytes */{}'.format(size))
                    self.send_header('Content-Length', '0')
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    self.close_connection = True
                    return
                try:
                    # Fail before sending 200/206 if the first requested piece is unavailable.
                    parts = owner.store.read(owner.file_index, start, end - start + 1)
                    first = next(parts) if send_body else b''
                except (OSError, TorrentError):
                    self.send_error(503, 'Torrent source unavailable')
                    return
                self.send_response(status)
                self.send_header('Content-Type', owner.mime_type)
                self.send_header('Content-Length', str(end - start + 1))
                self.send_header('Accept-Ranges', 'bytes')
                self.send_header('Connection', 'close')
                if status == 206:
                    self.send_header('Content-Range', 'bytes {}-{}/{}'.format(start, end, size))
                self.end_headers()
                self.close_connection = True
                if send_body:
                    try:
                        self.wfile.write(first)
                        for content in parts:
                            self.wfile.write(content)
                    except (OSError, TorrentError):
                        self.close_connection = True

        class Server(ThreadingHTTPServer):
            daemon_threads = True

        self.httpd = Server(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:{}/{}/video{}'.format(self.httpd.server_port, self.token, self.extension)

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=3)
