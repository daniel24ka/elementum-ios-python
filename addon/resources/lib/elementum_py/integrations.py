"""Interoperate with existing Kodi scrobblers and Up Next through their APIs.

Accounts, authorization, token refresh and scrobbling are owned by the official
Trakt/SIMKL add-ons. No credentials are read or copied by this integration.
See THIRD-PARTY-INTEGRATIONS.txt for upstream interfaces and licenses.
"""

import base64
import datetime
import json

from .catalog import CatalogError, image, media_item, plugin_url

COMPANIONS = {'trakt': ('script.trakt', 'Trakt'),
              'simkl': ('script.simkl', 'SIMKL TV Tracker'),
              'upnext': ('service.upnext', 'Up Next')}


def identifiers(item):
    if not isinstance(item, dict) or not item.get('id'):
        raise CatalogError('Catalog item has no valid identifier')
    external = item.get('external_ids') or {}
    result = {'tmdb': str(item['id'])}
    for key in ('imdb', 'tvdb'):
        value = item.get(key + '_id') or external.get(key + '_id')
        if value:
            result[key] = str(value)
    return result


def playback_metadata(catalog, query):
    """Keep a show's identifiers distinct from its episode's identifiers."""
    if query.get('show') and query.get('season') is not None and query.get('episode') is not None:
        show_id, season, episode = int(query['show']), int(query['season']), int(query['episode'])
        show = catalog.details('tv', show_id)
        details = catalog.api('/tv/{}/season/{}/episode/{}'.format(show_id, season, episode))
        result = media_item(show, 'tv')
        result['info'].update({'title': details.get('name') or '', 'plot': details.get('overview') or '',
                               'tvshowtitle': show.get('name') or '', 'season': season, 'episode': episode,
                               'mediatype': 'episode', 'premiered': details.get('air_date') or ''})
        ids = identifiers(show)
        result['uniqueids'] = identifiers(details)
        result['properties'] = {'tvshow.' + name + '_id': value for name, value in ids.items()}
    elif query.get('tmdb') and query.get('type') == 'movie':
        details = catalog.details('movie', int(query['tmdb']))
        result = media_item(details, 'movie')
        ids = identifiers(details)
        result['uniqueids'] = ids
        result['properties'] = {name + '_id': value for name, value in ids.items()}
    else:
        return None
    # Subtitle add-ons such as Wizdom ask the player for "imdbnumber": the film's
    # IMDb id, or the show's for an episode. Without it they guess from the title
    # through a second service, and show an empty list whenever that guess fails.
    if ids.get('imdb'):
        result['info']['imdbnumber'] = ids['imdb']
    # Trakt's window property holds movie IDs or SHOW IDs for episodes. SIMKL
    # also accepts these properties directly on the resolved ListItem.
    encoded = json.dumps(ids, separators=(',', ':'))
    result['properties'].update({'script.trakt.ids': encoded, 'script.simkl.ids': encoded})
    return result


def next_episode(catalog, show_id, season, episode, today=None):
    """Find the next aired episode, including season transitions, excluding specials."""
    today = today or datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    show = catalog.details('tv', int(show_id))
    candidates = sorted({int(entry['season_number']) for entry in show.get('seasons', [])
                         if int(entry.get('season_number', 0)) > 0 and int(entry['season_number']) >= int(season)})
    for number in candidates[:100]:
        episodes = catalog.api('/tv/{}/season/{}'.format(show_id, number)).get('episodes', [])
        for item in sorted(episodes, key=lambda entry: int(entry['episode_number'])):
            position = int(item['episode_number'])
            if number == int(season) and position <= int(episode):
                continue
            aired = item.get('air_date') or ''
            if len(aired) == 10 and aired <= today:
                return number, item
    return None


def _episode_info(show, episode, season):
    return {'episodeid': 'tmdb:' + str(episode['id']), 'tvshowid': 'tmdb:' + str(show['id']),
            'title': episode.get('name') or '', 'season': int(season), 'episode': int(episode['episode_number']),
            'showtitle': show.get('name') or '', 'plot': episode.get('overview') or '',
            'playcount': 0, 'rating': episode.get('vote_average') or 0,
            'firstaired': episode.get('air_date') or '', 'runtime': int(episode.get('runtime') or 0) * 60,
            'art': {'thumb': image(episode.get('still_path')), 'tvshow.clearart': '', 'tvshow.clearlogo': '',
                    'tvshow.fanart': image(show.get('backdrop_path'), 'w1280'),
                    'tvshow.landscape': image(show.get('backdrop_path')), 'tvshow.poster': image(show.get('poster_path'))}}


def upnext_payload(catalog, query):
    if not query.get('show') or query.get('season') is None or query.get('episode') is None:
        return None
    identifier, season, episode = int(query['show']), int(query['season']), int(query['episode'])
    following = next_episode(catalog, identifier, season, episode)
    if following is None:
        return None
    next_season, details = following
    show = catalog.details('tv', identifier)
    current = catalog.api('/tv/{}/season/{}/episode/{}'.format(identifier, season, episode))
    return {'current_episode': _episode_info(show, current, season),
            'next_episode': _episode_info(show, details, next_season),
            'play_url': plugin_url('/show/{}/season/{}/episode/{}/autoplay'.format(
                identifier, next_season, details['episode_number']))}


def upnext_notification(payload):
    """The documented JSONRPC.NotifyAll contract; no AddonSignals dependency."""
    return {'sender': 'plugin.video.elementum.SIGNAL', 'message': 'upnext_data',
            'data': [base64.b64encode(json.dumps(payload).encode('utf-8')).decode('ascii')]}
