Elementum iOS Python — development port

Status: beta. Supports movies/TV, Burst source searches, v1 magnets and torrent playback.
The original native engine's advanced feature set is not fully reproduced.
This repository does not distribute a replacement Kodi application.

Kodi source URL:
https://daniel24ka.github.io/elementum-ios-python/

Install in an existing Kodi 19+ installation:
1. Settings > File manager > Add source: use the URL above.
2. Add-ons > Install from zip file > choose that source.
3. Install repository.elementumios.python-0.1.1.zip.
4. Install from repository > Elementum iOS Python > Video add-ons > Elementum.
5. Install/enable Elementum Burst under Program add-ons.
6. Open Elementum under Video add-ons.

Implemented: original Elementum Kodi frontend, movie/TV catalog, Burst callback
integration, general/movie/episode search, v1 magnets and metadata exchange,
HTTP/UDP trackers, iterative DHT peer lookup, persistent TCP peer transfers,
piece integrity checks, HTTP range streaming, file selection, disk/memory cache,
playback history and resume, and basic movie STRM library entries.
Memory cache is the default, so watching does not retain entire films on disk.

Validation: local protocol/API integration tests, public Ubuntu/Sintel peer
metadata and verified pieces, plus Sintel picture and sound on ordinary Kodi
21.3 on a physical iPhone with HTTP webseeds disabled.

Not yet equivalent to the original native engine: Trakt/SIMKL integrations,
all maintenance/API routes, full multi-torrent download/seeding management,
uTP/MSE, BitTorrent v2 and automatic episode sequencing. Do not describe this
development build as the complete original Elementum engine.

The existing Kodi application must remain validly signed. This add-on does
not sign or renew Kodi. Kodi must stay in the foreground for iOS playback.

Source credits:
Elementum UI/Python helpers: https://github.com/elgatito/plugin.video.elementum
Elementum engine/API behavior: https://github.com/elgatito/elementum
Burst: https://github.com/elgatito/script.elementum.burst (WTFPL)
Python module archives: official Kodi Omega add-on repository; licenses are
preserved inside their source ZIPs. Main upstream Elementum license: MIT.
New Python backend: MIT, 2026 Elementum iOS development contributors.

This product uses the TMDB API but is not endorsed or certified by TMDB.
Sintel is the Blender Foundation's Creative Commons test film.

Tests: python -m unittest discover -s tests -v
No paid code-signing service or remote torrent server is part of this build.
