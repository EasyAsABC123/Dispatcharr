# Faster Rust development and milestone validation

Original Dispatcharr and dependency authors retain all application, algorithm,
model and protocol credit. React remains unchanged.

Use three persistent work lanes alongside the integrating agent: implementation,
Source/Native acceptance, and test infrastructure. Assign complete feature groups
and their dependent fixes; immediately queue the next independent work when a lane
finishes. The integrating agent owns the candidate, commits, application fixtures
and measurement window. Four concurrent agent slots are currently available.

## Short development loops, coherent acceptance batches

1. Characterize previously unmeasured Source behavior once. Include actual React
   payloads, errors, field coercions, headers, signals, task descendants and cleanup.
   Preserve observations before assertions and reuse the resulting authority.
2. Develop related changes together. During development use formatting, affected
   crate compilation and meaningful focused controls. Collect wire fixes before
   freezing the release candidate; avoid full workspace/build cycles per tiny fix.
3. Freeze one source snapshot, run the release development gates, and build its
   image once. Reuse the single Cargo target and Docker caches. Compilation can
   overlap one functional fixture when input independence and resource headroom
   are verified. It cannot overlap performance measurement.
4. Run one acceptance batch selected by the dependency graph. Candidate67's
   planned batch covers preview, dummy creation, Schedules Direct mutation/search,
   channel-guide coupling, range, enabled logos and normal version tasks. These
   eight contexts cover the changed interfaces and prior failed cells; ten is only
   the new Source HTTP capture count, not the whole Native workload.
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
