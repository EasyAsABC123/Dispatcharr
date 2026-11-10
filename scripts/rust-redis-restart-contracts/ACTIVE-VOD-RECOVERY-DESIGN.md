# Active VOD recovery design and cause gates

Original Dispatcharr authors retain full application and algorithm credit. This is an unimplemented proposal after the valid image34 mismatch166. No production code or stop/fence policy is changed by this document. Tests and builds wait until the performance quiet window ends.

## What is established

166 keeps the actual HTTP response and immutable candidate process alive while only owned Redis restarts without persistence. Original delivers278,528correct prefixbytes without EOF and reopens the same ID with200; Rust ends after65,536correct bytes and returns503 `No available stream`. This is a failed active-continuity gate, not a failure of the separately passed connection/reply-ownership tests159/161. Original HTTP metadata/providercount can remain1 after a bounded1.5seconds following wire close; do not infer a permanent leak or complete backend drainage from that sample.

The native pump exits on Session.stop. It does not directly observe Lease.lost. SessionPool.monitor runs once per second and sets Session.stop when its lease is lost, its metadata owner differs/missing, a stop key exists, or the probe errors. Once set, this watch is sticky. Existing same-ID registry entries are reused before new-session allocation; `state.closed || session.stop` returns503 before a fresh provider acquire. Coordinator heartbeat also makes its own lease lost/stopped on a failed heartbeat. Therefore reconnecting transport cannot revive the old body by itself.

The65536-byte prefix is consistent with one paced origin write followed by cancellation before the next; the exact monitor branch in166 is an inference, not yet instrumented proof. Do not attribute the branch merely from the absence of Redis state.

## Exact current-behavior tests before a policy change

Add a test-only overlay, preserving the159/161 production bytes:

1. Create a real SessionPool with two request slots and a live ticket. Keep the coordination owner/ledger/providercount and socket healthy, remove only its metadata hash, and verify there is no stop key. Observe Session.stop while Lease.lost remainsfalse and Redis PING succeeds. Hold the ticket, then call same-ID enter with one free request slot; assert the exact503 body. This isolates missing metadata from provider capacity and sticky lease loss.
2. Keep matching metadata but remove the owned coordination owner, call the actual lease heartbeat to establish lost, then observe Session.stop and same-ID503. Record old-ledger/provider cleanup separately. This isolates the lease-lost branch.
3. Hold the original session state mutex while replacing its lost server records with an actual second SessionPool's same-ID session and fresh coordination lease. Release the mutex; the original must stop. Dropping the old ticket must preserve the new metadata token, new owner, live providercount and credential reservation. Finish the new session normally. No arbitrary foreign counter deletion is an acceptable production recovery strategy; deletion in this fixture only establishes the controlled state-loss precondition.
4. Keep metadata/owner intact, set the actual stop key, and prove recovery never clears or bypasses it. A real bounded dropped/held probe reply can separately isolate the Redis-I/O branch without claiming it is equivalent to absent state.

These cause tests are component proofs; the existing actual Source/native HTTP166 remains the paired visible-behavior baseline. After an implementation, repeat the same actual HTTP protocol plus foreign owner, capacity and delayed-reply cases; no header/IP167 run is needed to establish the body/status mismatch.

## Proposed bounded recovery state

A session needs an explicit local state: healthy, recovering, or terminal, with at most one owned recovery operation per session. Acquire a global bounded recovery slot before spawning work. Caller cancellation abandons only the observer. Keep session/request ownership and the old provider HTTP body while the bounded recovery settles; do not set Session.stop at the first recoverable observation. Existing local body/frame budgets and backpressure continue to apply. Administrative stop, foreign ownership and failed physical cleanup remain terminal rather than silently permitted exceptions.

Capture immutable session identity, content/profile/credential context, metadata token, local active request count and generation before recovery. VOD currently supplies no credential pool to its coordinator request; retain that fact rather than inventing one. Any future credential-bearing request must carry its captured counter key/limit through recovery unchanged.

Classify server state without overwriting it:

| Observed state | Allowed next operation |
| --- | --- |
| Stop key present | Stop; never recover by deleting the key. |
| Foreign coordination owner or foreign/legacy metadata token | Stop/refuse; never adopt or overwrite. |
| Metadata missing, current lease still proven owned | Rebuild only missing metadata through a fenced atomic command; no second provider increment. |
| Metadata, coordination owner and this lease's ledger all missing | Try a fresh normal acquire with the original profile/credential limits, then guarded metadata creation. |
| Own ledger still reserves quota but owner/lease is lost | Explicit partial-state case: either a separately verified atomic reservation transfer or bounded refusal. Do not double-count by blindly acquiring again or zero shared counters. |
| Backend unavailable, type corruption, or ambiguous reply | Keep the owned operation/guards until actual reply/disconnection. Retry only a new classification or future safe operation; never repeat an uncertain mutation. |

A first small implementation can use the existing supervised `Coordinator.acquire_vod` for the all-missing case, followed by a metadata CAS that also checks the newly acquired owner. It must release the fresh lease if metadata became foreign between the two steps. This gives conservative temporary reservation, not a cross-Redis/PostgreSQL transaction. The metadata creation must use the existing bounded user-limit admission semantics; it must not bypass other users/sessions found after reconnection. A single combined Lua transition is another option, but would require all existing provider/credential/ledger invariants and failure gates again.

The old immutable Lease and its sticky lost watch cannot simply be marked healthy. Introduce a separately reviewed mutable ownership generation, and install the new lease/token only after its physical acquisition and metadata outcome are known. Retain the old lease until its own fenced cleanup completes. Old lease ledgers are token-specific, so a late release must not delete the new owner or decrement a replacement reservation. Never reset a shared fence counter; random tokens remain essential when a server restart resets numeric counters.

## Generation and body races that need proof

New same-ID admissions should wait on the bounded owned recovery, not create unbounded work or immediately reject merely because the old generation was stopped. Existing body delivery can continue within its normal memory/backpressure limits during recovery; it must not reopen the provider or restart at byte0. No result may report an owned session before the actual acquisition/CAS reply is settled.

An old Progress update, Ticket drop, idle timer, pending create, stop, or shutdown must not mutate a foreign or newer metadata generation. A state snapshot can become stale while a body closes, so restoring `active_streams` from an earlier snapshot needs a fenced reconciliation before admitting a new request. Preserve lock ordering; do not introduce a registry/state/ownership mutex cycle or hold unbounded work behind a borrowed caller future. Shutdown must close recovery admission and retain all physical guards through completion or its existing global deadline outcome.

Accurate rebuilding of locally active counts/provider reservations can differ from Original's absent/partially recreated public counters after state loss. Source166 exposes only the newly created session count while an old body still exists. A continuity fix with stronger quota bookkeeping must document and separately compare those visible metadata/statistics/event differences; it cannot be called complete1:1 parity merely because bytes continue. Avoid inventing start/stop events during silent recovery; same-ID reopen event semantics also need actual comparison.

## Required acceptance

Nominal Redis-only state loss: old HTTP body continues its exact prefix without EOF or duplicate bytes; same-ID reopen matches the observed Source response; no provider reopen or process restart is hidden. Then cover competing foreign owner, full profile/credential capacity, positive user caps, partial owner/ledger loss, late old cleanup, lost recovery acquisition/metadata replies, caller cancellation, concurrent old-body close/reopen, and SIGTERM while recovery is pending. Verify bounded slots/bytes and physical drainage along with metadata/counter/event effects. Keep failure/evidence scopes distinct and do not infer performance gains.

## Coordination physical API prerequisite

The following is a static API gap, not a retraction of the accepted152 acquisition proof. Source locations are in `rust/crates/coordination/src/lib.rs`:

| Current path | Boundary to test after quiet |
| --- | --- |
| `AcquireDelivery::Drop`, approximately170-191 | Already owns its admitted command permit through a direct release-script future without an inner response deadline. Preserve the existing cancellation proof. |
| `Lease::release_provider`,462; timeout470 | Borrowed CLEAR_PROVIDER future is dropped at the logical timeout; its local permit can be available while the actual reply is pending. |
| `Lease::reserve_provider`,493; timeout520 | Same issue for RESERVE_PROVIDER, including profile/credential counters and the reserved ledger field. |
| `heartbeat_inner`,588; timeout593 | A logical timeout can return while a heartbeat actually succeeded; public heartbeat then marks its lease lost. Do not confuse unknown reply with proven foreign ownership. |
| `release_inner`,611; timeout616 | Explicit release and ordinary `Lease::Drop`573 call this borrowed timed future. Spawning an outer task with a caller guard does not fix the inner deadline. |

`Coordinator::is_idle()` observes available command permits. It is not currently an actual-reply barrier for the above timed paths. A recovery loop must not see timeout/idle and immediately start another reservation or release while the prior mutation is unsettled.

Postquiet order: run the exact current cause tests first; next add real proxy gates holding/dropping each heartbeat, CLEAR_PROVIDER, RESERVE_PROVIDER and RELEASE reply after its actual server effect. Capture the current failure before changing APIs. Check caller ownership, command admission, observer outcome, ledger reserved field, owner token, profile/credential counts, no replay and foreign replacement survival. These are separate from150's shared-transport publication tests and152's initial acquire/drop supervisor.

A proposed narrow API carries an owned command permit, caller scope and immutable lease identity through a physically supervised operation, with separately observable actual completion. Existing logical-result behavior can delegate to it, while recovery waits on the physical completion receipt before classifying/retrying. A returned logical error is not that receipt. Late results must carry the ownership generation they probed; an old heartbeat/probe cannot stop a newly installed owned generation. Never introduce a generic retry of timed-out mutations.

## Source counters and the owner-CAS transition

A healthy same-ID Source session can have multiple active request bodies with one profile reservation. Do not multiply the provider/credential increment by the local request count. After full state loss166, Source's old body is unrepresented until another request creates fresh public state; that new metadata/count does not include every old body. Rebuilding accurate local counts is a stronger bookkeeping behavior and must be disclosed/tested separately from byte continuity.

Distinguish these controlled cases rather than treating every missing hash as a server restart:

* Only metadata missing, live matching owner/ledger still reserve quota: restore only under that proven identity; a fresh acquire would double-count or fail its own capacity.
* Owner/ledger/metadata all missing: fresh ordinary fenced acquisition is possible if the real shared caps allow it. Other sessions may already have recreated the shared counter; never assume that counter must also be absent or reset it.
* Matching metadata survives but the old lease/owner is missing or its watch is lost: not equivalent to the all-missing case. The old ledger may still reserve quota, or a logical heartbeat error may have marked a still-owned lease lost. This needs a separately proven owned reservation/identity transfer, or explicit bounded refusal; ordinary acquire alone cannot solve it safely.
* Foreign owner or metadata: refuse recovery and preserve it, regardless of whether another field is absent. A corrupt ledger/counter is not permission to zero shared state.

For a two-step acquire/metadata prototype, the metadata Lua must check the *new current coordination token* and absence of stop/foreign metadata in the same atomic step as admission/creation. A prior read is not this fence. If the metadata result conflicts, release only the newly acquired token/ledger and wait for its physical outcome. If its reply is ambiguous, classify the actual current token after settlement rather than repeat creation blindly. Preserve user admission and scan bounds as well as provider/credential caps.

If an owned partial-state transfer is later added, its atomic predicate must verify old owner token, old unique ledger/token/profile/credential/reserved fields and the expected metadata token. Rotation installs a new random coordination token, unique ledger, fence and metadata generation without a second quota increment when an existing reservation is actually transferred. Remove/retire only the verified old ledger. A queued old RELEASE then sees its old ledger absent; old CLEAR_PROVIDER/heartbeat cannot match the new owner. This is a design option requiring actual adversarial proof, not an authorized shortcut around a foreign owner or full cap.

Live local bodies may explicitly migrate into the successfully owned new generation. Operations already queued retain their old tokens. New operations take a current identity snapshot and retain it through the actual reply. Bump the local idle-cleanup epoch on migration, reconcile active counts after bodies that closed during recovery, and prevent old pending-create/idle callbacks from deleting the new generation. Continue using the existing provider response/byte cursor; no byte0 reopen is part of ownership recovery.

Adversarial gates must include a foreign worker winning between classification/acquire/CAS, a replacement while an old operation reply is held, old cleanup after a new ledger/token, positive caps and profile/credential conflict, actual stop key appearing during recovery, a body closing while its metadata snapshot is in flight, same-ID reopen before and after recovery, late old successful/failed heartbeat, and shutdown with recovery physical work still owned. Test partial failures at both transition replies. A nominal no-persistence continuity pass alone does not close these cases.
