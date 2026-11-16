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

Version 0.2.3 reliability update: 50 passing packaged tests, including a real
TCP range read past 20 GiB in a synthetic 24 GiB torrent, 8 MiB piece transfer,
80 concurrent seek requests, cancellation and repeated start/stop cycles.
Supports v1 piece lengths up to 64 MiB with bounded prefetch concurrency;
replaces unusable peers, discovers trackers/DHT concurrently, and prefetches
codec indexes at the tail of the selected file. Catalog requests share a cache.
Source searches close progress dialogs and expose useful errors without
automatically replaying a failed torrent request. Some external search sites
require browser verification and some swarms have no usable sources.
On a physical iPhone, a 30,323,795,308-byte video started and advanced after a
one-hour seek with webseeds disabled. This verifies start/seek/stop, not an
entire movie playthrough. Another 2.4 GB source timed out during startup;
availability and peer transfer quality still vary by source.

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
