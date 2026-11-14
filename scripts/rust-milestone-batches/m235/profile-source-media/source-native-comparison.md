# Custom FFmpeg input: measured Source and Rust scope

All application credit belongs to Dispatcharr’s original authors and contributors.

The pinned Original and current Rust image were exercised independently with the
same custom profile cloned from Original’s built-in FFmpeg parameters, the same
local H264/AAC MPEG-TS fixture, two viewers across two replicas, provider capacity1,
and the same shared-demand, global-stop and owner-crash contexts. Their capture
files are different executions, not simultaneous paired performance samples.

| Observation | Original1241b3f7 / pinned34f901 image | Rust1dc16f8d / e88cc73 image |
| --- | --- | --- |
| Concurrent viewers per context | 2 across2 replicas | 2 across2 replicas |
| Actual input processes / upstream connections / provider slots | 1 /1 /1 | 1 /1 /1 |
| Captured MPEG-TS | Six HTTP200 samples, zero packet sync errors | Six HTTP200 samples, zero packet sync errors |
| Real decoder output | Six samples:74 frames each, audio+video | Six samples:74 frames each, audio+video |
| Reader disconnect | Remaining viewer continues; owner preserved | Remaining viewer continues; owner preserved |
| Global stop | Actual200 expected message; resources retire | Actual200 expected message; resources retire |
| Reader-loss retirement observation | 8.279s | 5.471s |
| Global-stop retirement observation | 0.616s | 0.508s |
| Owner-crash retirement observation | At90.461s: owner absent, upstream closed, process absent; provider_slots1 and input_index4 remain | Automatic resources retire at28.504s |
| Owned fixture teardown | Actual0, five fresh independent absence checks0 | Actual0, five fresh independent absence checks0 |

The first five rows establish the measured shared-input/media compatibility slice.
Recovery has a **measured behavioral difference**: Original retains the reservation
and index at90seconds under this declared fixture boundary; Rust retires them. Recovery full parity is
not claimed. These wall times are lifecycle observations, not controlled CPU/RSS
performance comparisons or advertised gains.

Original raw SHA256:
`4a8b38d7996ac0a05e247683de886f82773e58bbdff8f327135a0f1c2c7250fa`.
Rust raw SHA256:
`ec401dd9f349111128e3bfe00cb70e9053adce99e250f9420e6d0046e1d2536c`.
The corresponding compact publication summaries retain their raw failure markers:
Original `passed:false` is intentional characterization; Rust’s broader required
node remains failed because the remaining domains were not accepted. Historical
Rust publication’s Source-media blocked marker is preserved literally; this later
Source capture qualifies only this exact FFmpeg/MPEG-TS topology.

VLC, active output profiles, provider regex/credential pools and custom-input
HLS/fMP4 remain unmeasured. Resolver32/33 is a Rust bounded-capacity safety result,
not a newly invented Original contract. Full M3 and performance acceptance remain
open. No inference is made from the18 argument vectors to runtime media formats.

The actual Source service boundary starts PostgreSQL, Redis, two uWSGI masters
with one stream worker each, the baseline web supervisor and deterministic
origin. It does not start Celery, Beat or Daphne, and overrides the packaged
whole-application entrypoint. The crash kills the actual baseline uWSGI group;
its DB/Redis and surviving replica stay available. Pinned
`apps/proxy/live_proxy/runtime.py` explicitly enables cleanup/pubsub in
uWSGI/gunicorn and makes Celery/Daphne client-only. `server.py` starts the
stream-worker cleanup loop, uses heartbeat and ownership TTL30s and a30s orphan
metadata sweep. `config.py` sets the TS cleanup check to1s. Fixture configuration
sets shutdown delay2s, chunk retention60s and behind-live5s. No Celery/Beat
live-orphan cleanup task was identified in the pinned task/settings code search.
These are code-backed runtime eligibility/timers, not instrumentation proving
every cleanup callback ran. Observed90s retention is bounded to this exact
fixture; it does not prove Original never cleans in every full deployment.
