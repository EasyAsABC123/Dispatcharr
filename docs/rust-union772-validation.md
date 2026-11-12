# Scoped scanner recovery and idle-reservation observer union

Full code, algorithm and license credit remains with Original Dispatcharr and dependency authors. React and the existing license are preserved.

The frozen 772-input Rust union passes strict whole-workspace/all-target Clippy, 558 default test executions across 170 suites, and the separately executed external-fixture gates below. There are 692 ignored fixture tests in the default invocation; this is not a claim that every ignored fixture ran. The default count includes ten observer controls compiled into multiple integration harnesses.

| Executed scope | Result |
| --- | --- |
| Finite Python repr / actual extracted JSON sizing helper | 9,368 values, 46,840 typed comparisons, zero differences |
| Scratch/filesystem/independent-process controls | 12 pass; one fixture child remains intentionally ignored |
| Real Redis and component HTTP VOD controls | 80 pass, including four-second success, busy and client loss |
| Spool service against Original 259-file reference | 544 checks and four physical controls pass |
| Original producer through spooled worker and process-loss recovery | 560 checks pass |
| Spooled first-scan helper | 80 checks pass; four separate existing helper physical controls use the legacy memory harness |
| Portable scanner reconstruction/builder/runner corruption controls | 18 pass |

All three owned worker projects and the VOD fixture were independently checked absent. Default-database audits pass. Exact source, executable and report hashes are published under `scripts/rust-integration/union772/`; VOD component evidence is under `scripts/rust-vod-pending-main/idle-phase-observer/verified-union772/`.

The explicit spooled constructor keeps at most 32 records per processing batch and uses a bounded 256KiB private spool plus a 136-byte ownership control. It admits up to 4,096 entries within the existing 64KiB total-path and 32-directory bounds. It fully enumerates and validates before effects, preserving observed traversal order. The old constructor retains its 128-file bound. This is metadata spooling, not a new media payload cache.

Recovery acquires an exclusive lock on a stable control inode and removes only the exact validated recorded dead-run scratch. The original descriptor-close implementation could leave the lock held by an unrelated child between fork and exec. The new guard explicitly unlocks after settled cleanup and on error/drop paths, refuses stale-owner reuse, and retains active-owner exclusion. A deterministic inherited-descriptor witness and 50 parallel cohorts establish this mechanism and correction. Production adds no unsafe code; the controlled fork witness is test-only. Unknown/foreign files are retained and refused. Partial initial bootstrap, power loss, external directory mutation and general concurrent application behavior remain outside this contract.

One prior fixed771 service run missed the unchanged ten-second SQL-unblock drain assertion. Its cause was not captured and remains unresolved historical evidence. Monitored and fresh portable771 runs, and the first fixed772 cohort, pass separately. No timeout was widened, no failed run was relabeled, and host pressure is not asserted as its cause. The historical770 packet is preserved separately from `spool-recovery-unlock`.

The idle VOD observer now accounts separately for acknowledged provider reservation. It retains the three-second control budget before/after that phase and uses a Source-derived first-read observation window of 60 seconds while the physical reservation settles. The ordinary coordinator's two-second limit and other recovery observers are unchanged. Authoritative scalar timestamps prevent coalesced notifications from resetting budgets; an over-budget completed reservation remains expired. Caller/observer loss does not abort or replay the physically owned mutation. This does not establish Original retry behavior, total-request deadlines or actual 60-second physical command parity.

Packaged Main validation on a new immutable image is the next gate. Image50's valid four-second failure remains published. Recovery/default worker intake remain off; M3U/EPG scanner branches, the complete worker graph, full M4/M6 and numerical performance acceptance remain open. This source qualification does not transfer image48's measured gains to a newer image.
