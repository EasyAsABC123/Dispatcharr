# Faster Rust development and milestone validation

Original Dispatcharr and dependency authors retain all application, algorithm,
model and protocol credit. React remains unchanged.

Develop across milestones in parallel. The four current lanes cover playback
(M1–M4), API and protocol behavior (M5 and M7), providers and jobs (M6), and
baseline, operations and release (M0, M8 and M9). Each lane owns its changed
paths and uses focused checks tied to pinned Python behavior. A milestone
dependency controls final acceptance. It does not put another lane's code
changes on hold.

Integrate the lanes into one release candidate for the final end-to-end run.
Use the [existing milestone coordinator](../scripts/rust-milestone-batches/README.md)
to track the required cells, candidate bindings, and retained Python evidence.
Keep the covered, failed, blocked and unrun cells visible. Use the same
candidate for Compose acceptance, then measure paired workloads in a quiet
window. Run the soak and rollback rehearsal on the release images and copied
data. Close milestones only after their required gates pass.

## Short development loops, coherent acceptance batches

1. Characterize previously unmeasured Source behavior once. Include actual React
   payloads, errors, field coercions, headers, signals, task descendants and cleanup.
   Preserve observations before assertions and reuse the resulting authority.
2. Develop related changes together. During development use formatting, affected
   crate compilation and meaningful focused controls. Collect wire fixes before
   freezing the release candidate; avoid full workspace/build cycles per tiny fix.
3. Freeze one source snapshot, run the release development gates, and build its
   image once. Reuse the single Cargo target and Docker caches. Before Cargo
   opens a shared target, refresh the input modification times in the private
   Git-archive snapshot and verify its file bytes still match the committed
   inputs. Git archives inherit commit timestamps, which can predate artifacts
   from a different candidate and cause Cargo to return stale results.
   Candidate104's Docker build detected a missing constructor field after a
   cached local run appeared to pass. Run the ordinary binaries with
   `cargo check --locked -p dispatcharr-rs --bins` before the focused tests and
   retain the actual Docker build as the packaging gate. Compilation can
   overlap one functional fixture when input independence and resource headroom
   are verified. It cannot overlap performance measurement.
4. Run one acceptance batch selected by the dependency graph. Candidate67's
   planned batch covers preview, dummy creation, Schedules Direct mutation/search,
   channel-guide coupling, range, enabled logos and normal version tasks. These
   eight contexts cover the changed interfaces and prior failed cells. Two
   current-image live/VOD viewing smokes bring the Native selection to ten
   contexts; this is separate from the ten newly captured Source HTTP requests.
5. Keep unaffected, immutable component results as explicitly scoped baseline
   evidence. Run broad integration at coherent release boundaries and when shared
   behavior changes. A smaller batch cannot silently claim a whole milestone.
6. After functional acceptance, reserve a quiet window for paired Source/Native
   performance. Include compatibility workers, media children, database/cache and
   declared observer costs, with equal useful work and unchanged error/swap rules.

Source Unicode normalization, raw UI custom properties and DRF success headers
have already exposed bugs before a new application run. Pure dependency controls
help development; they do not replace actual HTTP, database or shutdown acceptance.

## Overlap independent functional fixtures safely

A default-off proposal keeps the canonical PRIMARY lock. Existing exclusive
owners and performance remain exclusive. Opted-in Source-only and Native-only
owners share PRIMARY while each holds an exclusive backend slot. This allows at
most one Source and one Native fixture; contexts within a fixture stay serial
because Redis resets and data symlinks are shared.

Eight [actual temporary-directory kernel checks](rust-development-validation-controls.json),
with ten child waits and EOF,
verify mutual exclusion, separate backend slots, partial-acquisition cleanup and
a durable cleanup-uncertainty interlock. This is not real fixture isolation proof.
Before activation, run a bounded two-fixture smoke checking actual networks,
ports, databases, mounts, projects, process generations, ancestor resource limits
and both cleanup receipts. Require the exclusive performance lease afterward.
The current frozen Native25 run remains exclusive and unchanged.

The proposal's admission floor is twelve GiB available memory and eight allowed
CPUs. Container caps can total nine GiB and eight CPU quota units; controllers,
Docker overhead and ancestor limits still need measured headroom. Do not compile
alongside an opted-in two-fixture pair or accept performance gains from overlap.

The first real overlap launched both owned Compose projects. Original failed
at its read-only source mount. The controller killed the Native owner while it
was waiting, leaving its disposable project. A separate host recovery retired
its exact four containers, network, volume, and process generations, released
the loopback port, and removed the matching cleanup marker. The revised harness
creates the media mount point and lets a remaining owner clean up when its peer
fails. Full isolation and resource admission require the revised live run.
[First overlap and recovery receipt](rust-functional-overlap-first-attempt.json)

## Remove repeated observation startup

The recorded Native22 controller took 3,563 seconds. Its 5,500 command receipts
sum to 2,177 seconds; those receipts overlap and include intentional work, so this
is not an estimate of avoidable startup time. The final Comskip interval is about
1,213 seconds and includes required recording/media and three Main epochs.

Instrument explicit context setup/business/teardown times. A separate proposal
combines existing before-boundary SQL, cache and broker reads into one Docker exec
and Django setup, retaining the original scripts, order, fences and custody. It
adds no transaction or persistent observer and retains completed SQL evidence if
a later read fails. Independent after/failure capture remains separate. Validate
this in a short affected batch and measure actual setup/boundary savings before
expanding it. Required recording durations and recovery epochs stay intact.

Track development-check time, Source characterization time, affected-batch time,
broad-integration time, accepted cells per iteration and repeated unaffected work.
Compare like-for-like workloads before claiming any testing speedup. Build cache
hits and test infrastructure timings are separate from application CPU/memory gains.

All full M0–M9 milestones and current performance gains remain open. These changes
speed iteration and expose failures sooner; they do not weaken acceptance.
