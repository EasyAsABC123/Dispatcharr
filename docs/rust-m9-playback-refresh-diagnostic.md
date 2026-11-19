# Candidate91 playback during a guide refresh

The original Dispatcharr authors and contributors retain full application
and algorithm credit.

Candidate91 ran ten authenticated MPEG-TS readers continuously for 120
seconds with the programme refresh worker enabled. All 24 five-second
observations saw ten progressing audio and video readers and one new,
active connection to the local upstream. The Original and Native guide
requests ran during viewing. Native completed the owned XML programme job
and the separate source-less terminal case.

The paired Compose controller exited 0 with all 26 checks passed. The ten
reader connections and threads closed. All 20 bounded media clips passed
strict decoding after certifying their MPEG-TS edges. Each clip contained
194–206 decoded video frames and 387,072–403,200 audio samples. All four
owned containers and observed process generations were absent after cleanup,
along with the project network and volume.

The first controller exited 1 after observing the same playback and refresh
window. Intentional socket shutdown reached the observer as a chunked-body
error. Its first independent packet-aligned replay decoded 17 of 20 clips.
The later deterministic edge review decoded the three additional clips and
rejected mutations inside the certified media. The original exit is retained.

This diagnostic uses the finite MPEG-2 origin and the refresh-only configuration.
The two-hour soak, active worker graph acceptance and full M9 resource checks
remain open.

- [Exact successful retry receipt](../scripts/rust-integration/m9-playback-refresh-diagnostic/candidate91.json)
- [First-attempt result](../scripts/rust-integration/m9-playback-refresh-diagnostic/first-attempt.json)

## Candidate94 H.264/AAC qualification

The exact Candidate94 package ran a separate 120-second trial using the
long-lived H.264/AAC origin and the pinned VLC input profile. It passed all
24 Compose checks. Ten authenticated readers shared one upstream and one
encoder generation through all 24 observations. The two XMLTV programme
requests at +5 and +60 seconds returned HTTP 200 and reached distinct
expected SQL rows and Redis results. All 20 bounded captures verified five
complete seconds of both audio and video. The decoder and controller
exited 0. All reader sockets, the encoder generation and four owned Compose
container generations retired.

The trial qualified the longer origin and the two-job sequence. The
two-hour workload and its resource and retention budgets remain open.
[Candidate94 qualification receipt](../scripts/rust-integration/m9-playback-refresh-diagnostic/candidate94.json)

## Resource and shared-slot qualification

The first short resource trial recorded eleven complete cgroup and process
censuses for the four measured roles. The private relay closed all ten
viewer connections before an HTTP status arrived. The relay had a five-second
idle timeout, and the preceding successful trial recorded first media bytes
between 6.28 and 6.31 seconds. The controller exited 1 and removed all owned
resources. The corrected fixture keeps the existing 15-second client deadline
and allows 16 seconds of relay inactivity. A local delayed-response test passed.
A full resource window and the two-hour soak remain open.
[First resource trial receipt](../scripts/rust-integration/m9-playback-refresh-diagnostic/resource-short-attempt1.json)

The second short trial passed the relay startup and recorded twelve
five-second observations. All ten viewers delivered MPEG-TS from the same
upstream through the 60-second observation and retired cleanly. At 60 seconds
the infrastructure process census returned an incomplete inventory. Rust,
origin and relay censuses completed at that point. The controller exited 1
and exact Compose cleanup passed. The next observer retries a full census
within the same sampling deadline when process membership races. Rejected
attempts remain in its journal, and permission failures still fail the census.
[Second resource trial receipt](../scripts/rust-integration/m9-playback-refresh-diagnostic/resource-short-attempt2.json)

The third short resource trial passed all 28 checks. Ten viewers shared
one upstream and one provider slot through the full 120 seconds. Both
XMLTV jobs reached their distinct durable results. All 20 media clips
passed strict decoding. The observer completed 25 active counter ticks
and 11 complete censuses in each idle window. At 60 seconds the baseline
process inventory changed three times. A complete fourth census passed
within the original bound. Post-idle resource limits passed, and all five
owned container generations retired. This result qualifies the scoped
resource and provider setup for the two-hour diagnostic.
[Passing short resource receipt](../scripts/rust-integration/m9-playback-refresh-diagnostic/resource-short-attempt3.json)

## First two-hour attempt

The exact Candidate94 run reached the first scheduled XMLTV refresh. All
ten viewers shared one upstream through the last complete playback tick at
290 seconds. The 295-second resource sample completed. The refresh failed
before the observer could open at 297 seconds. The controller retained a
`ValueError` without the HTTP status or failed validation step. It exited 1,
and exact Compose teardown passed. The diagnostic retry captures the stage,
status and allowed response-shape flags without including credentials.
[First two-hour attempt receipt](../scripts/rust-integration/m9-playback-refresh-diagnostic/long-attempt1.json)

The second two-hour attempt used the same playback and refresh schedule.
The first scheduled Native password request returned HTTP 500 without an
access and refresh token pair. The XMLTV POST was never issued. The actual
controller exited 1 and exact Compose teardown passed. Separate owned
reproductions compare the elapsed-time boundary with and without active
viewers.
[Second two-hour attempt receipt](../scripts/rust-integration/m9-playback-refresh-diagnostic/long-attempt2.json)


## Five-minute authentication isolation

Both repaired Candidate94 diagnostic runs completed and cleaned up their
owned Compose resources. The ordinary Native password request returned
HTTP 500 with `Authentication cache unavailable` after 300 seconds in two
conditions. The stationary condition had zero viewers and zero upstream
connections. In the viewing condition, all ten viewers continued through
one upstream connection and one provider slot. Neither diagnostic submitted
an XMLTV request.

The auth throttle history key was absent before and after both requests.
Redis recorded no increase in error replies across either request. These
observations locate the failure in the shared auth throttle path. The exact
Redis client error class still requires direct verification. The successor
adds a single retry when the bounded history read observes a dropped Redis
connection. The compare-and-set write still has one physical attempt.

[Paired diagnostic evidence](../scripts/rust-integration/m9-playback-refresh-diagnostic/password-cache-diagnostic.json)


## Candidate103 password regression

The exact Candidate103 image completed both natural cooldown cases with
HTTP 200 at five minutes. The stationary case then admitted two more
attempts, returned HTTP 429 for the fourth, and verified the stored
throttle history and expiry were preserved at the limit. The viewing case
recorded a single history append during ten advancing viewers. It kept
the same provider slot and upstream generation during the request.

Both owned Compose projects and their observed process generations were
removed. The directed single-Redis-connection test and the full two-hour
soak remain pending.

[Successor natural-auth evidence](../scripts/rust-integration/m9-playback-refresh-diagnostic/password-cache-successor.json)
