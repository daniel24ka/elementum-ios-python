Elementum iOS Python — development port

Status: 0.3.3 beta. Supports movies/TV, Burst source searches, v1 magnets and torrent playback.
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

Native-engine parity remains incomplete: full maintenance/API routes, seeding,
uTP/MSE and BitTorrent v2 remain pending. Companion integrations and persistent
downloads are described below, with their validation limits.

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

Version 0.3.0:
- Persistent on-device download queue, 1-3 simultaneous jobs subject to a shared
  memory budget, pause/resume/cancel, safe deletion and free-storage reservations.
- Select the primary video or all torrent files from a source context menu.
- Restarted unfinished jobs stay paused until explicitly resumed.
- Verify saved pieces offline; resume/retry repairs corrupt or missing pieces.
- Completed downloads play and seek from verified local pieces without discovery,
  peer connections or webseeds. Streaming cache remains separate from saved jobs.
- Optional Web dashboard for download management and saved playback, protected
  by a rotating access token. Localhost by default; local Wi-Fi is opt-in.
  Open Elementum > Web interface for the address and token. Keep the token private.
  A browser on the same iPhone may suspend Kodi when switched to the foreground;
  use Kodi's Downloads menu directly, or opt in to Wi-Fi for another device.
- Optional official Trakt, SIMKL and Up Next companion integration. Account
  authorization and scrobbling are owned by those separately installed add-ons.
  No Trakt/SIMKL account or credential is required for Elementum itself.
- Movie/episode IDs reach the resolved player item without confusing show IDs
  with episode IDs. Up Next can request the next aired episode across seasons.
- Episode-pack resume positions are separated by file; movie STRM entries now
  resolve a source for playback. Stop/replacement clears companion properties.
- Keep the responsive metadata peer first when opening video connections.
- Process peer control messages while idle and wait for unchoke rotations.
- Accept previously requested late blocks after chokes; keep strict hash checks.
- Consume PEX peers, refresh discovery and replace unusable connections within
  the eight-connection budget. Record safe startup phase/error diagnostics.
- Verify both the head and tail pieces before handing a video URL to Kodi.
  A source with an unavailable seek index fails during preparation, allowing
  controlled recovery instead of a generic player-opening error.
- Try at most two alternative sources for the same movie or episode when
  startup fails. Never increase a known selected resolution; honor cancellation
  and resolver time limits. This can be disabled in Elementum settings.
- Cache up to four verified metadata results briefly; reserve upload slots
  for foreground reads/seeks and interrupt pending peer preparation on cancel.

Validation: 124 packaged protocol/API/stress tests, including real peer download,
pause/restart/resume, corrupt-cache recheck and repair, offline range playback,
storage rejection before transfer, Web auth/origin/deletion checks and recovery.
Signed-in Trakt/SIMKL scrobbling and real-device Up Next transitions have not
been verified. Companion settings and source availability can affect behavior.

Still pending: uploading/seeding management, uTP, MSE/PE transport encryption,
BitTorrent v2/hybrid torrents, full native-engine feature/API parity and full TV
STRM export. This is an independent Python port, not the native Elementum engine.

Version 0.3.2 subtitles:
- A dedicated native Kodi subtitle entry point replaces the incorrect video
  menu previously displayed as subtitle results. Normal video search is intact.
- Search movie/show IMDb identifiers and episode numbers via the public
  OpenSubtitles Stremio endpoint; prefer Hebrew and retain Kodi language choices.
- Display language, provider/release, hearing-impaired and matching indicators.
- Use the original torrent filename and an available cached movie hash for
  matching, without requesting extra torrent pieces for subtitle hashing.
- Download validated SRT/ASS/VTT subtitles, including ZIP responses; normalize
  UTF-8/BOM, UTF-16/32 and Windows-1255 Hebrew without reversing logical text.
- Subtitle style, size, visibility and timing use Kodi's native controls.
- Network/cache/archive limits and atomic bounded caches protect the player
  from malformed provider responses. Provider availability and timing vary.
- Existing Ktuvit/Wizdom subtitle providers remain separate and unmodified.

Physical validation on ordinary iPhone Kodi 21.3: Sintel native subtitle search
returned seven Hebrew/English results, a Hebrew SRT downloaded and loaded as an
enabled Hebrew track, and the viewer confirmed readable Hebrew on screen.
This verifies this film/provider path; it does not guarantee subtitle coverage
or frame-exact synchronization for every movie or release.

Subtitle integration adapts the MIT-licensed SubBridge project:
https://github.com/subzero75/kodi-subbridge
Its original license and modification notice are included in the add-on.
No subtitle text, account credentials or device identifiers are distributed.

Version 0.3.3 playback optimization:
- Continuously replenish a bounded verified read-ahead window instead of
  scheduling only the first few pieces. Adapt its size to video bitrate,
  transfer observations and buffering, within a 16-64 MiB cache (32 by default).
- Give a new long seek range the active window; cancel obsolete queued work.
  Keep in-flight reads valid, ignore small codec-tail probes for window focus,
  and reserve one available upload slot for foreground reads and seeks.
- Rank sources using local observed startup success/failure and throughput,
  retaining Burst's existing quality slots. Store only hashed source identities
  and bounded aggregate history, expiring after seven days; never persist URLs.
- Recover an empty-buffer stall after 25 seconds without playback advancement
  and 15 seconds without verified data, or a reported playback error. Pauses,
  seeks, manual stops, raw unidentified magnets and near-end playback are guarded.
- Try at most two remembered same-title alternatives, skip already attempted
  sources and higher known resolution, and resume the resolved replacement at
  the saved position. This is best-effort timeline matching across releases.
- Add Optimize playback buffer to the Kodi menu. It chooses a bounded native
  cache from available memory, preserves larger custom/disk caches, enables
  native adaptive reads when replacing the default, and backs up prior settings.
  Kodi settings change only when this action is invoked, not on every startup.
- Distinguish verified torrent read-ahead from native Kodi cache progress in
  diagnostics. A short white seek-bar segment on a large video can be normal:
  the same memory buffer represents a smaller fraction of a larger file.

Validation: 124 packaged tests include rolling refill, seek priority, memory
bounds, service-loop stall/reopen/seek and pause/stop guards, plus all previous
protocol, 24 GiB addressing, cancellation, download and subtitle regressions.
Automatic mid-playback failure handling is covered by controlled service-loop
injection; it is not a guarantee of recovery from every real swarm outage.

Physical validation: ordinary iPhone Kodi 21.3 played Sintel through P2P with
webseeds disabled, detected H.264/AAC streams, advanced throughout the sample,
grew its verified torrent cache to 31.94 MiB within the 32 MiB limit, and resumed
after an eight-minute seek. The requested native cache action selected 64 MB
and adaptive reads, with prior settings backed up. Sequential swarm runs had
changing peer availability and different sample durations; these observations
do not establish a universal throughput or startup-time improvement.

Design references (independent implementation):
https://www.libtorrent.org/streaming.html
https://kodi.wiki/view/Settings/Services/Caching
https://github.com/xbmc/xbmc/blob/Omega/xbmc/filesystem/FileCache.cpp
