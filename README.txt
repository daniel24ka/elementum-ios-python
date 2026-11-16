Elementum iOS Python — development port

Status: 0.3.4 beta. Supports movies/TV, Burst source searches, v1 magnets and torrent playback.
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

Version 0.3.4, published in the repository. It is the test build 0.3.4~dev2
with its version number changed. The test builds ran on an iPhone; what that
showed, and what ~dev2 fixed, is at the top of CHANGELOG.txt:
- Subtitle add-ons such as Wizdom now receive the IMDb number of what is
  playing, and the service survives being switched off and on or updated.
- Every piece is downloaded block by block from all unchoked peers at once, up
  to 20 connections. The piece a reader waits for comes first, a silent peer is
  set aside after about a second, and a corrupt block is traced to its sender.
- Idle peers are kept and sent keep-alives. 0.3.3 dropped every peer that was
  silent for ten seconds and never dialed it again.
- The cache no longer evicts read-ahead before it is read. In 0.3.3 a full
  cache downloaded each piece almost four times (200 pieces, 755 fetches).
- The duration is read from the Matroska/MP4 header, so about eight seconds of
  the actual video are buffered before playback, and a seek index stored after
  the media is fetched at the same time.
- During a stall a small verified reserve is released slowly, a session Kodi
  ended early is reopened at the same position, and local listeners are
  reopened after an iOS suspension. These three rest on reading Kodi's source
  and Apple's documentation; they have not been observed on a device.
- Thirteen built-in public trackers, peers used as each tracker answers, DHT
  nodes remembered between sessions, saved metadata reused, BEP 6.
New settings: Start buffer before playback (0-16 MiB), Peer connections while
streaming (4-40). CHANGELOG.txt lists every change.

Controlled benchmark (tools/swarm_bench.py; benchmarks/2026-11-16-swarm.json):
scripted peers on loopback TCP with real rates and faults, and a reader that
requests ranges like Kodi and plays at the video bitrate. Medians; 0.3.3 ran
twice and 0.3.4~dev1 three times per scenario, alternating, on Windows 11 with
Python 3.12. Seconds to start / seconds to resume after a seek / stalls.

  scenario                              0.3.3                 0.3.4~dev1
  10 peers x 400 KiB/s                  1.9 / 2.2 / none      1.3 / 1.7 / none
  16 peers x 150 KiB/s                  13.5 / 19.3 / 7.5x 44 s   3.1 / 1.7 / none
  2 fast + 8 very slow peers            25.5 / seek failed    2.5 / 1.8 / none
  2 of 6 peers go silent                4.1 / 7.1 / 6.5x 17 s     2.6 / 1.7 / none
  8 MiB pieces, 2.5 MiB/s video         13.7 / 32.5 / 7x 73 s     3.8 / 2.2 / none
  4 useful of 14 peers, 30 dead         6.2 / 5.1 / 4x 9 s        6.5 / 1.6 / none
  half the swarm has 30% of the file    6.8 / 9.0 / 7x 20 s       2.7 / 2.0 / none
  every peer drops after 6-14 s         5.1 / 6.2 / none      2.7 / 2.2 / none

What the table does not show: peak process memory rose by 4 to 21 MiB (61 to
65 MiB in the first scenario); start-up in the sixth scenario is unchanged,
because it is spent waiting for dead addresses to time out; these are scripted
peers, not a public swarm, and the reader is a model of Kodi, not Kodi.

The table was measured before the review fixes described in CHANGELOG.txt; one
more pass of all eight scenarios on the final code gave the same picture (no
stalls, 1.2-6.3 s to start, 1.6-2.5 s after a seek) and is stored in the same
file. The benchmark was repeated once on 0.3.4~dev2 with the same result (no
stalls, 1.2-6.2 s to start, 1.6-2.5 s after a seek). 252 tests pass against the packaged code.

Real-swarm check from a PC (tools/real_swarm_check.py, Sintel, peers only): one
run of each version on the same evening. 0.3.3 was ready after 19.8 s and read
1.1 MiB/s from 6 peers; the final 0.3.4~dev1 was ready after 2.3 s, read the
whole 123 MiB film at 9.8 MiB/s from 12 unchoked peers (all of them BEP 6
clients), read the duration (888 s) from the file, and answered a seek in 0.1
s; 16 KB of 173 MiB arrived twice. Public swarms change by the minute, so this
shows that the new peer layer works with real clients; it is not a measurement
of how much faster it is.

Done on a physical iPhone (Sintel, regular Kodi 21.3): start, seek, stop,
subtitles through Wizdom, and switching the add-on off and on. Still to do
there: a full-length film; a forced stall (source cut, Wi-Fi change) to see
the trickle and the same-position resume; leaving Kodi and returning; memory
while playing a torrent with 8-16 MiB pieces; saved downloads; Wizdom on an
episode.

Tools: python tools/package_addon.py --repo . --version 0.3.4~dev2 --output dist
       python tools/validate_package.py dist/<zip> --out VALIDATION.json
       python tools/swarm_bench.py run --lib addon/resources/lib --out new.json
       python tools/swarm_bench.py compare old.json new.json

Design references (independent implementation):
https://www.libtorrent.org/streaming.html
https://kodi.wiki/view/Settings/Services/Caching
https://github.com/xbmc/xbmc/blob/Omega/xbmc/filesystem/FileCache.cpp
