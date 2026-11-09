# Testable Timeshift migration steps within M4

All original application credit belongs entirely to the original Dispatcharr
authors and contributors. These steps do not replace the repository milestone
acceptance gates; M4 remains in progress until its functional and resource
requirements all pass.

| Step | Current evidence | Required next gate |
| --- | --- | --- |
| Source contract capture | 207 timestamp/timezone/URL vectors; 246 range/header vectors; 24 native API wire cases; six actual media/provider scenarios, three strict A/V decodes | Extend the source captures for busy seeks, idle adoption, stop reasons, profile rotation and XC layouts |
| Native session identity | Four real Redis/HTTP tests, 22 source API wire cases, actual Django Store/Auth/JWT integration | Remaining native errors/serializer/OPTIONS variants and WebSocket effects |
| Provider transport | Four real HTTP tests for fallback, partial bytes, 416, failure classification, cached CDN behavior and bounded request lifetime | Frozen app integration and all provider redirect/encoding/failure cases |
| Native capability media and pool | Compiled/strict Clippy passed; four native HTTP, five pool and one cancellation test and actual SQL/JWT/native HTTP/source-hash/grace/strict decoder (74 frames) passed | Freeze app image and extend adoption/seek/caps/telemetry before full media parity acceptance |
| Positive shared admission | Three Redis tests, six original-helper differential cases, positive real Store/Auth native refusal/retry and frozen-image byte/decode/grace comparison passed | Broader user policy/stop effects and reuse/adoption matrix |
| Complete source pool/control behavior | Pending | Idle fingerprint adoption, busy seeks, media re-anchor, generation handoff, cached final URL, cross-worker state, shared positive user caps/preemption, all stop reasons, complete telemetry |
| App/Compose equivalence | Default-Proxy capability slice passed:24 API,6 provider/media, positive-limit refusal/retry, four candidate strict A/V decodes; image/source binding and cleanup | Route the proven engine, freeze source/image manifest, compare native and XC wire/media/control behavior against the same source database and origin |
| Resource comparison | Pending | Freeze workload/budgets before execution; paired repeated source/Rust trials with equivalent side effects and valid decoded output |

The next executable native media gate is
`python scripts/rust-vod-contracts/run.py --timeshift-rust-media --output artifacts/rust/<new-directory>`.
It passed in `verified-native-media.json`. It
runs a host Rust HTTP service against disposable Docker PostgreSQL/Redis/origin;
it does not by itself establish a frozen application-image comparison.

Resource trials must use an archive long enough to outlast warmup and sampling.
The current 60-second contract fixture is intentionally short and cannot serve
as the continuous playback performance fixture. Generate the long immutable
archive before the quiet window, record its codec/mux/hash/rate and use identical
Range starts and provider/user budgets in both backends. Keep each viewer body
open across warmup and measurement so API revocation/grace does not silently
turn a throughput trial into repeated authentication failures. Test cold-open
and reconnect behavior separately.

Measure 1/10/100 native viewers over three paired, alternating trials, using the
repository's frozen warmup/duration and budget format. Report selected-stack and
component CPU, RSS/cgroup memory, delivered bytes, startup/throughput, error and
decoder results. Include the origin and database/Redis services, stop the inactive
backend and record all image/source/configuration bindings. A faster subset
without equivalent user admission/statistics is characterization only; it must
not mark application performance verified.

Viewer identity must also be explicit: source Timeshift can fingerprint-adopt
an existing busy pool when user/media match and both IP and User-Agent match
(score8). A loopback load generator that repeats one User-Agent may therefore
measure seek/handoff of one provider stream instead of independent viewers.
Distinct fingerprints avoid adoption but the source then terminates other
same-user/same-channel stats sessions as a programme hop before user-limit
checking. Use distinct fixture users for independent viewers, assert
actual provider/session counts in both backends, and retain the source-adoption
scenario as a separate functional/load workload. Do not compare a source
adopted stream against 100 independent Rust upstreams and call it fanout parity.
