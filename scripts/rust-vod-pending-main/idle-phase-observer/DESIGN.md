# Private idle observer phase proposal — physical replay not authorized

Base is immutable Git208598ee only; no working-tree VOD bytes. Two runtime files
and one test file change. No Main/Cargo/coordinator/shared Recovery edits.

## Authority and actual6-second result

Pinned Original core/utils.py RedisClient uses socket_timeout60, connect5 and
retry_on_timeout=true. apps/m3u/connection_pool.py reserve_profile_slot synchronously
awaits INCR; the existing-session0→1 path in multi_worker_connection_manager.py
awaits it before get_stream. There is no aggregate3s handler timer in that path.
The fixed4s Source busy response429 arrived at4.010429 seconds. Source success200
arrived at6.012497: provider get_stream follows reservation, and the owned origin's
PacedWriter delays its first65536-byte write by65536/32768=2 seconds. Source's
stream generator consumes8192 and uWSGI delivers response headers with body output.
This explains the extra2 seconds from the actual fixture/code; it is not a6s Redis
policy or justification for an arbitrary6s/10s timeout.

Native image50 retained the correct physical reserve result but enter_idle_inner
still put the entire retained task behind Recovery.observer_timeout3s. Actual
native503 at3.005392/3.004334 preceded its held reply release. Ordinary Coordinator
2s is a different observer and remains unchanged.

## Exact proposed policy

The existing3-second CONTROL observation budget remains active before reservation
and after settlement through serial reacquisition/commit/rollback. Only the one
already-acknowledged idle provider reservation phase pauses that budget. It has a
separate60-second Source-first-read OBSERVATION window. This does not implement
Requests/redis-py retry/socket-reset semantics or promise complete parity after
that first window. Source uses a per-socket read timeout; native proposes an owned
phase observation bound including settlement, so equality is only qualified for
captured pre-timeout commands, not arbitrary multi-command/credential waits.

An authoritative mutex contains request start, optional reserve start and total
settled reserve duration. One watch cell is a wakeup only. The task publishes its
reserve phase under the mutex BEFORE polling the physical mutation. Each observer
poll recomputes remaining time from timestamps, including when phase notifications
coalesce. A phase begun after control exhaustion cannot forgive the earlier time.
The reserve guard closes the measured phase after physical settlement and before
serial/commit. No source result is invented. Typed full/error propagate unchanged.

Observer expiry drops only its JoinHandle; Tokio's spawned task, original oneshot,
body/caller/control guards and fenced lease retain physical ownership. Caller loss
has the same existing detached-task semantics. No mutation retry/replay, command
cancellation, pool expansion or success from timeout. No global timeout change.
The ledger/control/body caps already bound admitted jobs. This adds one scalar
mutex/watch cell per admitted idle job, no event queue or retained body payload.
A lost reply remains physically owned; shutdown cannot claim drain while pending.

## Validation and review boundary

Ten pure controls cover phase accounting, original remaining3s after reserve,
pre-expired control, finite60s boundary, coalesced notification, typed busy/error,
observer expiry retaining work, caller-observer loss and pre/post control expiry.
Short-clock async tests exercise the same helper, not actual Redis ownership.
Three new ignored actual HTTP fixtures define success/busy/client loss with default
Coordinator2s and physical reply held4s; they are authorized for component replay by Root after revision2 sticky expiry repair.
Existing retained-result tests cover ordinary2s and actual error/lost reply;
those must replay on this exact closure alongside idle/follower/stop/drain gates.
No transfer of old766 execution, no Main/package/full-M4 acceptance claim.

Revision2 preserves prototype1. Completed physical reserve duration >=60s remains
expired after reserve guard drop. Deterministic scalar control covers coalesced
start/end at60/61s plus59s below boundary. Actual component validation is authorized;
packaged Main replay is not.

Final exact revision2 validation:10 pure controls and strict coordination/vod
all-target ClippyPASS; actual old77 +new4s HTTP3 =80PASS on newly built exact
ELFs. Independent exact-owned bind cleanupempty. Packaged Main and actual60s
Source read/retry boundary remain unqualified.
