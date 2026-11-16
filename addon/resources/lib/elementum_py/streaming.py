"""Loopback-only HTTP range streaming for the experimental Python engine."""

import hmac
import re
import secrets
import socket
import threading
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .core import TorrentError

TYPES = {'.mp4': 'video/mp4', '.m4v': 'video/mp4', '.mkv': 'video/x-matroska',
         '.avi': 'video/x-msvideo', '.webm': 'video/webm', '.mov': 'video/quicktime', '.ts': 'video/mp2t'}
# Kodi's file cache treats about ten seconds without a single byte as the end of
# the file once its own buffer is empty, and playback then simply stops. A small
# verified reserve is therefore held back and released slowly during a stall, so
# the player keeps showing "buffering" while the swarm or a new source recovers.
RESERVE = 64 * 1024
DRIP = 256
HEARTBEAT = 1.0
WRITE_STEP = 1.0   # Seconds a response waits for the player before it looks whether it was dropped.


def retire_listener(httpd, thread):
    """Stop an accept loop, then free its socket. Never raises.

    Closing the socket alone would leave serve_forever() polling a dead
    descriptor, which spins at full speed on platforms that use poll().
    shutdown() waits for the loop to answer, so it is asked only while the
    loop's thread runs: one that never started would never answer, and one
    that has ended needs nothing.
    """
    if httpd is None:
        return
    running = thread is not None and thread.is_alive()
    try:
        if running:
            # What shutdown() does, without its unbounded wait and without a
            # helper thread, which may not be available when threads are short.
            httpd._BaseServer__shutdown_request = True
            if thread is not threading.current_thread():
                httpd._BaseServer__is_shut_down.wait(2)
    except AttributeError:
        pass   # Not a socketserver loop; its socket is all that can be freed.
    finally:
        try:
            httpd.server_close()
        except OSError:
            pass
    if running and thread is not threading.current_thread():
        thread.join(timeout=1)


def start_listener(server_class, address, handler_class):
    """A bound listener whose accept loop runs, or an exception and nothing left behind.

    A port that is bound while nobody serves it looks alive to every check
    and answers no request.
    """
    httpd = server_class(address, handler_class)
    try:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
    except BaseException:
        httpd.server_close()
        raise
    return httpd, thread


def listener_alive(port):
    """True when a loopback listener still accepts connections.

    A refused connection is certain. A timeout is not, because a busy phone can
    delay one accept; it gets a second try before a healthy listener is replaced.
    """
    for _ in range(2):
        try:
            socket.create_connection(('127.0.0.1', port), timeout=1).close()
            return True
        except ConnectionRefusedError:
            return False
        except OSError:
            pass
    return False


def stream_mime(url):
    """MIME type of one of this engine's own stream URLs, otherwise None."""
    match = re.fullmatch(r'http://127\.0\.0\.1:\d{1,5}/[a-f0-9]{48}/video(\.[a-z0-9]{2,4})', str(url or ''))
    return TYPES.get(match.group(1)) if match else None


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
    def __init__(self, store, file_index, heartbeat=HEARTBEAT, reserve=RESERVE, drip=DRIP, closing=None):
        self.store = store
        # Set when the whole service stops. It also reaches the responses of a
        # stream that an ordinary stop closed earlier and nobody refers to any more.
        self.closing = closing or threading.Event()
        self.file_index = file_index
        self.heartbeat, self.reserve, self.drip = heartbeat, reserve, drip
        self.token = secrets.token_hex(24)
        extension = os.path.splitext(store.meta.files[file_index]['path'][-1])[1].lower()
        self.extension = extension if extension in TYPES else '.mp4'
        self.mime_type = TYPES.get(extension, 'application/octet-stream')
        self.metrics_lock = threading.Lock()
        self.metrics = {'requests':0,'delivered_bytes':0,'source_errors':0,'last_source_error':0,
                        'stall_seconds':0,'dripped_bytes':0,'open_responses':0}
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
                owner.count('requests')
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
                parts = None
                first = b''
                try:
                    # Fail before sending 200/206 if the first requested piece is unavailable.
                    if send_body:
                        parts = owner.store.read(owner.file_index, start, end - start + 1, heartbeat=owner.heartbeat)
                        for first in parts:
                            if len(first):
                                break
                except (OSError, TorrentError):
                    owner.source_error()
                    if parts is not None:
                        parts.close()
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
                    owner.count('open_responses')
                    try:
                        self.connection.settimeout(WRITE_STEP)
                        # Hand the first chunk over without keeping a reference here:
                        # this frame lives as long as the response, the piece must not.
                        holder, first = [first], None
                        self.deliver(parts, holder)
                    except TorrentError:
                        owner.source_error()
                    except OSError:
                        # Kodi closes old range sockets during seeks; that is
                        # not a failed torrent and must not trigger recovery.
                        pass
                    finally:
                        parts.close()
                        owner.count('open_responses', -1)

            def write(self, data):
                # A player that is paused, or that parked this connection during a
                # seek, reads nothing, and sendall() would wait for it without end.
                # Kodi waits for every thread of a stopped service, so the wait is
                # taken in steps and ends when the service drops its streams.
                view = memoryview(data)
                while len(view):
                    if owner.dropped or owner.closing.is_set():
                        raise OSError('The stream was closed')
                    try:
                        view = view[self.connection.send(view):]
                    except socket.timeout:
                        pass
                owner.count('delivered_bytes', len(data))

            def deliver(self, parts, holder):
                chunk = holder.pop()
                held = b''   # Verified bytes not sent yet; never more than the reserve.
                reserve = owner.reserve
                try:
                    while True:
                        size = len(chunk)
                        if size > reserve:
                            if held:
                                self.write(held)
                            self.write(chunk[:size - reserve])
                            held = bytes(chunk[size - reserve:])
                        elif size:
                            held += bytes(chunk)
                            if len(held) > reserve:
                                self.write(held[:len(held) - reserve])
                                held = held[len(held) - reserve:]
                        elif held:
                            # No verified piece arrived within the heartbeat.
                            self.write(held[:owner.drip])
                            held = held[owner.drip:]
                            owner.count('dripped_bytes', min(owner.drip, len(held) + owner.drip))
                            owner.count('stall_seconds', owner.heartbeat)
                        else:
                            owner.count('stall_seconds', owner.heartbeat)
                        chunk = None   # Do not keep a whole piece alive while waiting for the next one.
                        try:
                            chunk = next(parts)
                        except StopIteration:
                            break
                finally:
                    # The reserve is verified and in order: deliver it even when
                    # the source fails afterwards, so no accepted byte is lost.
                    if held:
                        self.write(held)

        class Server(ThreadingHTTPServer):
            daemon_threads = True
            allow_reuse_address = True

        self.server_class, self.handler_class = Server, Handler
        self.listener_lock = threading.Lock()
        self.closed = False
        self.dropped = False
        self.httpd, self.thread = start_listener(Server, ('127.0.0.1', 0), Handler)
        self.port = self.httpd.server_port
        self.rebuilt = 0
        self.url = 'http://127.0.0.1:{}/{}/video{}'.format(self.port, self.token, self.extension)

    def ensure_listening(self):
        """Reopen the listener on the same port if the system took it away.

        iOS reclaims the sockets of a suspended app; a listener does not come
        back by itself when Kodi returns to the foreground, and the stream URL
        the player holds would then refuse every reconnect.
        """
        with self.listener_lock:
            # A session that ended in the meantime stays closed: nothing would
            # ever close a listener that was reopened for it.
            if self.closed or listener_alive(self.port):
                return False
            retire_listener(self.httpd, self.thread)
            self.httpd = None
            try:
                self.httpd, self.thread = start_listener(self.server_class, ('127.0.0.1', self.port),
                                                         self.handler_class)
            except (OSError, RuntimeError):
                return False   # The port or a thread is not to be had yet; the next check tries again.
            self.rebuilt += 1
            return True

    def count(self, name, amount=1):
        with self.metrics_lock:
            self.metrics[name] += amount

    def source_error(self):
        with self.metrics_lock:
            self.metrics['source_errors'] += 1
            self.metrics['last_source_error'] = time.monotonic()

    def stats(self):
        with self.metrics_lock:
            return dict(self.metrics, listener_rebuilt=self.rebuilt)

    def close(self, drop=False):
        """Stop accepting connections.

        Responses that are already open normally run on while the player
        reads what is cached; a switch to another source relies on that.
        ``drop`` ends them within about a second. It is for a service that
        Kodi is stopping: a paused player reads nothing, and the thread that
        serves it would otherwise outlive the service.
        """
        with self.listener_lock:
            self.closed = True
            self.dropped = self.dropped or drop
            httpd, thread = self.httpd, self.thread
        retire_listener(httpd, thread)
