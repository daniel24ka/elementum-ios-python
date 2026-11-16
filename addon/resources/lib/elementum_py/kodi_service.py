"""Kodi service lifetime, dialogs, provider execution and playback history."""

import json
import os
import re
import threading
import time

import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

from .backend import Backend, BackendServer


class Kodi:
    def __init__(self, addon, monitor):
        self.addon = addon
        self.monitor = monitor
        self.dialog = None

    def setting(self, key):
        return self.addon.getSetting(key)

    def language(self):
        language = xbmc.getLanguage(xbmc.ISO_639_1) or 'en'
        return {'he': 'he-IL', 'en': 'en-US'}.get(language, language)

    def rpc(self, method, params):
        response = json.loads(xbmc.executeJSONRPC(json.dumps({'jsonrpc': '2.0', 'id': 1,
                                                             'method': method, 'params': params})))
        if 'error' in response:
            raise RuntimeError('Kodi request failed: ' + method)
        return response.get('result')

    def addons(self):
        result = self.rpc('Addons.GetAddons', {'type': 'xbmc.python.script', 'enabled': True,
                                              'properties': ['name', 'enabled']})
        return result.get('addons', [])

    def execute_addon(self, addon_id, params):
        self.rpc('Addons.ExecuteAddon', {'addonid': addon_id, 'params': params, 'wait': False})

    def open_settings(self, addon_id):
        xbmcaddon.Addon(addon_id).openSettings()

    def has_addon(self, addon_id):
        from .integrations import COMPANIONS
        if addon_id not in {entry[0] for entry in COMPANIONS.values()}:
            return False
        return xbmc.getCondVisibility('System.AddonIsEnabled({})'.format(addon_id))

    def companion(self, addon_id):
        from .integrations import COMPANIONS
        if addon_id not in {entry[0] for entry in COMPANIONS.values()}:
            raise ValueError('Unsupported companion add-on')
        try:
            companion = xbmcaddon.Addon(addon_id)
        except RuntimeError:
            xbmc.executebuiltin('InstallAddon({})'.format(addon_id))
            return
        companion.openSettings()

    def publish_ids(self, metadata, session=None):
        window = xbmcgui.Window(10000)
        snapshot = {}
        for name in ('script.trakt.ids', 'script.simkl.ids'):
            value = metadata.get('properties', {}).get(name, '')
            if value:
                window.setProperty(name, value)
                snapshot[name] = value
            else:
                window.clearProperty(name)
        if session:
            from .subtitle_context import build_context
            try:
                context = json.dumps(build_context(session, session['stream'].url), ensure_ascii=False)
                window.setProperty('elementum.subtitle.context', context)
                snapshot['elementum.subtitle.context'] = context
            except (ValueError, TypeError, KeyError, AttributeError):
                window.clearProperty('elementum.subtitle.context')
                xbmc.log('Elementum: subtitle matching details are unavailable', xbmc.LOGWARNING)
        return snapshot

    def clear_ids(self, snapshot):
        window = xbmcgui.Window(10000)
        for name, value in snapshot.items():
            if window.getProperty(name) == value:
                window.clearProperty(name)

    def input(self, prompt):
        return xbmcgui.Dialog().input(prompt, type=xbmcgui.INPUT_ALPHANUM)

    def choose_file(self, names):
        return xbmcgui.Dialog().select('Choose video file', names)

    def notify(self, text):
        xbmcgui.Dialog().notification('Elementum', text, time=5000)

    def progress(self, text):
        if self.dialog is None:
            self.dialog = xbmcgui.DialogProgress()
            self.dialog.create('Elementum', text)
        else:
            self.dialog.update(0, text)

    def progress_close(self):
        dialog = self.dialog
        self.dialog = None
        if dialog:
            dialog.close()

    def provider_health(self):
        try:
            path = xbmcvfs.translatePath('special://logpath/kodi.log')
            with open(path, 'rb') as handle:
                handle.seek(0, 2)
                handle.seek(max(0, handle.tell() - 65536))
                recent = handle.read(65536).decode('utf-8', errors='replace')
            domains = re.findall(r'\[script\.elementum\.burst\] Cloudflare challenge for ([a-zA-Z0-9.-]+):', recent)
            return {'blocked_domains': list(dict.fromkeys(domains))[-12:]}
        except OSError:
            return {}

    def abort_requested(self):
        return self.monitor.abortRequested() or bool(self.dialog and self.dialog.iscanceled())

    def resume(self, seconds):
        minutes, seconds = divmod(int(seconds), 60)
        return xbmcgui.Dialog().yesno('Elementum', 'Resume from {}:{:02d}?'.format(minutes, seconds))

    def log_error(self, kind, message):
        # Do not write search credentials, passkeys or callback tokens to Kodi logs.
        clean = re.sub(r'(?i)(api_key|token|password|passkey|authorization)=([^&\s]+)', r'\1=[redacted]', message)
        clean = re.sub(r'(?i)(https?://\S+|magnet:\?\S+)', '[source]', clean)
        xbmc.log('Elementum Python: {}: {}'.format(kind, clean[:500]), xbmc.LOGERROR)
        return clean

    def error(self, kind, message):
        clean = self.log_error(kind, message)
        self.notify((kind + ': ' if kind != 'TorrentError' else '') + clean[:220])

    def status(self, stats):
        xbmcgui.Dialog().textviewer('Elementum Python backend',
                                    'Version ' + self.addon.getAddonInfo('version') + '\n\n' + json.dumps(stats, indent=2) +
                                    '\n\nPython torrent engine; no additional native library.\n'
                                    'Feature parity with the original engine is still under development.')

    def confirm(self, text):
        return xbmcgui.Dialog().yesno('Elementum', text)

    def web_access(self, address, token):
        xbmcgui.Dialog().textviewer('Elementum Web interface',
            'Open this address in a browser:\n' + address + '\n\nAccess token:\n' + token +
            '\n\nKeep Kodi in the foreground. iOS may suspend connections when you leave Kodi. '
            'LAN access is optional in settings. The token controls downloads and playback; keep it private.')


def run():
    addon = xbmcaddon.Addon('plugin.video.elementum')
    monitor = xbmc.Monitor()
    kodi = Kodi(addon, monitor)
    profile = xbmcvfs.translatePath(addon.getAddonInfo('profile'))
    backend = Backend(profile, kodi)
    server = None
    published_ids = {}
    try:
        server = BackendServer(backend)
        if kodi.setting('web_enabled') == 'true':
            try:
                from .web import WebServer
                backend.web = WebServer(backend, lan=kodi.setting('web_lan') == 'true')
            except OSError:
                kodi.error('Web interface', 'The dashboard port is unavailable; Kodi menus remain available')
        xbmc.log('Elementum Python backend listening on loopback', xbmc.LOGINFO)
        player = xbmc.Player()
        tracked_url = None
        published_ids = {}
        was_playing = False
        last_save = 0
        while not monitor.waitForAbort(0.5) and not backend.shutdown.is_set():
            if kodi.dialog and kodi.dialog.iscanceled():
                backend.engine.stop()
                kodi.progress_close()
            session = backend.engine.active
            if session is None:
                kodi.clear_ids(published_ids)
                published_ids = {}
                tracked_url = None
                was_playing = False
                continue
            url = session['stream'].url
            playing = player.isPlayingVideo()
            if playing:
                try:
                    if player.getPlayingFile() != url:
                        if was_playing and tracked_url == url:
                            kodi.clear_ids(published_ids)
                            published_ids = {}
                            backend.engine.stop()
                            tracked_url, was_playing = None, False
                        continue
                    if tracked_url != url:
                        tracked_url = url
                        if backend.pending_resume:
                            player.seekTime(float(backend.pending_resume))
                            backend.pending_resume = 0
                        kodi.clear_ids(published_ids)
                        published_ids = kodi.publish_ids(session.get('metadata', {}), session)

                        def announce(expected=url):
                            try:
                                backend.announce_next_episode(expected)
                            except Exception as error:
                                kodi.error(type(error).__name__, 'Could not prepare Up Next episode details')

                        threading.Thread(target=announce, daemon=True).start()
                    was_playing = True
                    if time.monotonic() - last_save >= 10 and backend.state['history']:
                        backend.state['history'][0]['position'] = player.getTime()
                        backend.state['history'][0]['duration'] = player.getTotalTime()
                        backend.save()
                        last_save = time.monotonic()
                except RuntimeError:
                    pass
            elif was_playing and tracked_url == url:
                kodi.clear_ids(published_ids)
                published_ids = {}
                backend.engine.stop()
                tracked_url = None
                was_playing = False
    except Exception as error:
        kodi.error(type(error).__name__, str(error))
    finally:
        kodi.clear_ids(published_ids)
        kodi.progress_close()
        if server:
            server.close()
