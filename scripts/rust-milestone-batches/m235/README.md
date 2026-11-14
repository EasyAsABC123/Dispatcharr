# M2 / M3 / M5 acceptance batch

This registry connects the existing full 28 package runners with production Main,
unchanged React, live lifecycle, format/profile and settings-consumer workflows.
It is an executable adapter for **one Root/global dispatcher**. It starts no
parallel pool of its own. All owners share the same `package/<name>` IDs, global
three-fixture limit and exclusive heavy/resource leases.

Read the matrix or readiness without starting an application:

```sh
python3 batch.py --matrix
python3 batch.py --bindings /path/to/global-bindings.json --repo /path/to/repo
```

The global dispatcher imports `load_matrix()`, `readiness()` and
`resolve_node(id,bindings)`; then calls `run_node(id,bindings,global_context,runner)`.
`runner(job)` is the existing globally bounded subprocess/cleanup collector. A
single batch authorization and attempt journal cover all ready nodes. There are
no per-helper cues, flags, new Cargo targets or separate permission cycles.
Failed, blocked and not_run stay distinct; clean failures do not stop independent
nodes. Fixture-invalid descendants stay blocked. Actual source counts and
cleanup are collected before final acceptance. Aggregate acceptance requires
the full required matrix, including production-Main and performance cells; a
component or ignored test cannot fill them.

The exact 28 existing package names/argv are from image 57's reviewed definition.
That is a workload template, not candidate acceptance. `package_argv` replaces
only explicit manifest/image/output/support arguments. New strict per-node input
bindings must match the one new integrated candidate; no old guard is weakened.
Old Main55 labels/cues are deliberately excluded. Source 17 can be reused when its
Original/support/selector inputs match. The refresh reference's unchanged900 s
predicate requires a fresh reference followed by Native under the SAME global
lease. Guide starts only in the original minute 00–35 window. The final shared TS,
all-down and retention nodes run solo in order. No counts are forced to 24/14.

`source_workflow.py` is the concrete new Source-reference cohort adapter. One
PG/Redis bootstrap and one owned SQL clone are reused for a genuinely TS-cold
class sequence, one actual 2.4 s held Redis GET reply, one new persisted-key PATCH,
relevant 10 s expiry and 18 StreamProfile argument plus six owner-lock calls. It
calls unmodified Source methods; the old Source 32/51/Output 24 requests are not
repeated. Model setup and class contexts are explicitly separate. Profile argv
and owner-lock calls do not establish FFmpeg/VLC execution or process sharing.
The declared Source return statuses, errors and incomplete captures are retained,
not used to choose another request or retry. The global fixture context supplies
one nonce/output; no per-helper approval file is created.

Known active-profile/HLS-profile 501 routes are **failure probes**, not readiness
skips once a safe owned fixture/current candidate exists. Full profile delivery,
format/failover and task-consumer cells remain explicit blockers until implemented
and run. The new Source cohort completed with actual controller exit 0: a cold
TS slot survives Base invalidation, then refreshes at actual age 10.275289 s. Its
first Redis reply was physically held 2.400140 s, distinguishing pre-load cache
age from completion-time age. All eight cleanup stages passed. The earlier
Base-first cohort remains authority for inherited hits; its final 9.71 s age was
not expiry. The integrated Core/engine/Main health adapter follows both measured
class contexts. It remains temporarily default-off for unqualified exceptional
settings/error domains, and requires the joint compile and production Main batch.
See docs/rust-proxy-health-consumer.md and the compact Source evidence JSON.
Input/output retention freshness is a separate required workflow.

`matrix.json` lists production interfaces, source authority, dependencies,
assertions, execution/fixture boundaries and blockers. External M6 task-graph and
M9 datastore decisions are referenced, not silently accepted. Existing published
Source and Native evidence retains its exact old bindings; image 57 gains and
Core 892 component evidence do not transfer to a later Main. After functional
acceptance, representative whole-service paired workloads require one coordinated
quiet window and fixed budgets. No resource run is authorized by this adapter.

The migration-milestone-testing skill adds no approval requirement. This proposal
uses the current Root admission restriction only for actual app dispatch; no
helper, file read or local development check waits for separate permission.
Original Dispatcharr authors and dependency contributors retain their credit and
licenses. Source code references retain the pinned Original commit/hash map.
