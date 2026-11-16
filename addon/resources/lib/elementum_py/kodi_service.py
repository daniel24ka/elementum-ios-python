"""Kodi service lifetime, dialogs, provider execution and playback history."""

import json
import os
import re
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

    def abort_requested(self):
        return self.monitor.abortRequested() or bool(self.dialog and self.dialog.iscanceled())

    def resume(self, seconds):
        minutes, seconds = divmod(int(seconds), 60)
        return xbmcgui.Dialog().yesno('Elementum', 'Resume from {}:{:02d}?'.format(minutes, seconds))

    def error(self, kind, message):
        # Do not write search credentials, passkeys or callback tokens to Kodi logs.
        clean = re.sub(r'(?i)(api_key|token|password|passkey|authorization)=([^&\s]+)', r'\1=[redacted]', message)
        xbmc.log('Elementum Python: {}: {}'.format(kind, clean[:500]), xbmc.LOGERROR)
        self.notify(kind + ': ' + clean[:150])

    def status(self, stats):
        xbmcgui.Dialog().textviewer('Elementum Python backend',
                                    'Development version 0.2.0\n\n' + json.dumps(stats, indent=2) +
                                    '\n\nPython torrent engine; no additional native library.\n'
                                    'Feature parity with the original engine is still under development.')


def run():
    addon = xbmcaddon.Addon('plugin.video.elementum')
    monitor = xbmc.Monitor()
    kodi = Kodi(addon, monitor)
    profile = xbmcvfs.translatePath(addon.getAddonInfo('profile'))
    backend = Backend(profile, kodi)
    server = None
    try:
        server = BackendServer(backend)
        xbmc.log('Elementum Python backend listening on loopback', xbmc.LOGINFO)
        player = xbmc.Player()
        tracked_url = None
        was_playing = False
        last_save = 0
        while not monitor.waitForAbort(0.5) and not backend.shutdown.is_set():
            if kodi.dialog and kodi.dialog.iscanceled():
                backend.engine.stop()
                kodi.progress_close()
            session = backend.engine.active
            if session is None:
                tracked_url = None
                was_playing = False
                continue
            url = session['stream'].url
            playing = player.isPlayingVideo()
            if playing:
                try:
                    if player.getPlayingFile() != url:
                        continue
                    if tracked_url != url:
                        tracked_url = url
                        if backend.pending_resume:
                            player.seekTime(float(backend.pending_resume))
                            backend.pending_resume = 0
                    was_playing = True
                    if time.monotonic() - last_save >= 10 and backend.state['history']:
                        backend.state['history'][0]['position'] = player.getTime()
                        backend.state['history'][0]['duration'] = player.getTotalTime()
                        backend.save()
                        last_save = time.monotonic()
                except RuntimeError:
                    pass
            elif was_playing and tracked_url == url:
                backend.engine.stop()
                tracked_url = None
                was_playing = False
    except Exception as error:
        kodi.error(type(error).__name__, str(error))
    finally:
        kodi.progress_close()
        if server:
            server.close()
