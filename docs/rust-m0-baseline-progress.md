# Remaining M0 baseline work

Original Dispatcharr authors retain all application/algorithm credit and licenses.
M0 remains open for Source characterization and evidence accounting. It does not
require completing the later Native milestones first.

The [live benchmark](../scripts/rust-live-perf/image47/README.md) now includes
nine Source measurement phases at1/10/100 viewers:333 authenticated readers and
531 resource samples. The [same-runtime churn](../scripts/rust-live-perf/churn/README.md)
adds six Source rounds/33 readers without between-round restarts or Redis resets.
These support the baseline; they are not full worker-graph measurements.

| Existing scenario | Necessary remaining evidence |
| --- | --- |
| Idle before/after playback and reconnect churn | Corrected three-repetition reference now has valid coverage in all windows. Source growth budgets fail and remain recorded. Preserve these baseline facts; remaining work concerns the other scenarios and budget/contract accounting. |
| 1/10/100 viewers on one channel | Reconcile existing repeated warmed measurements with their exact budgets and preconditions; retain the cold100 failure separately. |
| Ten distinct channels and configured capacity | Warmed ten-channel Source accounting passes with limit0 explicitly unlimited. Separate configured capacity1 and10 references now verify shared reuse, refusal, release and later admission. They remain Source-only functional gates. |
| Slow plus fast readers | Qualified same-container accounting now records before/active/local-close/70s-retention states with the existing observer. It remains Source functional evidence, with no isolated resource acceptance. |
| Stall, failure, stream change and recovery | Stall/failover now have qualified observer/state/cleanup attachments. Interior warning and healthy recovery remain separate. Actual admin POST stream-change and same-container sustained15/60/70 references now pass, separately scoped without CPU/memory acceptance. |
| Import/EPG refresh during playback | One real bounded Source workload now verifies2 imports/6 tasks/44 durable checks during continuous playback. Worker-inclusive resource cost remains unmeasured; resource controller stays blocked pending qualification. |
| Sustained playback followed by disconnect | Reconcile same-run post-close retention/footprint evidence, or add that accounting to the declared sustained workload. An unrelated short cleanup run cannot substitute. |

The [public observer controller](../scripts/rust-baseline/idle-churn/README.md)'s short contract test passed18 offline controls, ten real observer
probe samples, nine complete three-service censuses and a2MiB prefix decoding124
frames. Same-UID read-only collectors avoid granting application privileges;
transition/incomplete samples stay recorded and are excluded from idle acceptance.
The full policy still requires five complete stable samples per20-second idle
window. Baseline/performance/M0 acceptance remains false until the full run and
remaining scenarios are reviewed. This protocol uses its declared retention and
service configuration; it is not every production configuration.

Budget bookkeeping must retain historical scope. The live experiment declared
relative CPU/memory/throughput/latency gates, zero errors/OOM, minimum sample
coverage and cleanup deadlines. It did not separately declare every absolute
startup or post-cleanup-growth ceiling. Record such fields as present, absent or
covered by a later policy; do not retroactively label a new limit predeclared.
No extra arbitrary-hour soak is added to M0; full soak/cutover remains M9.

Source failures remain authoritative: the cold100/reopen reference received no
bytes at its ten-second deadline. Original owner death still leaves a provider
slot after90 seconds in the [two-worker replay](../scripts/rust-integration/verified-shared-ts-image47-1.json).
The combined report remains failed despite the native recovery pass. The later
[retention replay](../scripts/rust-integration/verified-retention-image47-1.json)
is candidate-only and cannot become Source evidence by relabeling it.

The regenerated [contract register](rust-contract-register.md) retains592 routes,
798 operation items and250 non-HTTP candidates, with11 partial operation bindings
and403 scoped observations; fully audited operations remain zero. Bind existing
reports to exact IDs, methods, roles, fixtures, assertions and limits. Passive
metadata, help and Source-only captures are separate from Native passes. Closing
this bookkeeping does not require all798 items or all later role matrices to
become Native-verified, or fixing Original defects first.


[The first full Original idle/churn attempt](../scripts/rust-baseline/idle-churn/verified-original-three-repetitions-negative-1.json) completed102 readers and all18 workload phases, with independently empty owned projects. It remains failed. Repetitions1/3 recorded post-playback process growth1 and FD growth151/149, then post-churn process growth2, FD growth279/264 and socket growth28/26 against the sealed limits. Most process/FD growth belongs to the shared infrastructure service. Retained UID/PID aggregates do not distinguish live processes, zombies or individual descriptor causes; these are not causal leak findings. Memory growth stayed within budget.

Repetition2 is observer-invalid: a permission-denied read of `/proc/754/fd/0` stopped census sampling, leaving zero later stable idle samples. It supports no growth inference. The original policy, workload, failures, complete/incomplete samples and raw hashes remain unchanged. A separate private observer correction is being qualified to record denied coverage as incomplete and continue sampling, while persistent incomplete coverage must still fail the existing minimums. No Native comparison, gain or complete M0 acceptance follows from this attempt.


[The corrected full reference](../scripts/rust-baseline/idle-churn/verified-original-three-repetitions-corrected-2.json) completes all three unchanged-policy repetitions with valid stable coverage, no observer errors and all102 readers/18 workload phases passing. Two real denied reads are explicitly incomplete and excluded while later windows remain valid. Source growth-budget acceptance is still false: process growth1 after playback and2 after churn; corresponding FD growth151 and274/226/237; socket growth26 after churn. No thresholds changed and no repeat is required merely to seek green budgets.

The pinned Source deliberately keeps up to three warm database connections per worker, with maximum pool size8 and default lifetime600s. That is plausible context for infrastructure process/FD growth; retained UID/PID aggregates do not establish individual process state, descriptor ownership or a causal leak. This is valid characterization of the declared Original fixture, not Native gains, Source budget acceptance or complete M0 closure. The first observer-invalid run remains unchanged.


[The cumulative Source functional accounting attachment](../scripts/rust-playback-baseline/verified-source-accounting-1.json) covers mixed readers, stall, failover and warmed ten-channel playback. Existing observer/workloads remain scoped, incomplete/transition samples stay explicit, and callbacks record wall overhead rather than CPU cost. The exact many-channel replay ran after a fresh reviewed quiet cue; its earlier nonempty-output setup failure remains retained. All four scenarios reach zero final clients/upstreams. Absolute absent limits remain null; no Native or full M0/resource acceptance follows.


New Source-only references cover [capacity1](rust-m0-finite-capacity.md), [capacity10 with an eleventh distinct-channel refusal](rust-m0-finite-ten.md), [real admin POST stream change](rust-m0-stream-change.md), and [one continuous sustained reader with same-container close/retention](rust-m0-sustained-accounting.md). They pass12/30/10 checks respectively and the declared sustained read/decode/retention conditions, with22 portable guards across the four new harnesses. The sustained sampler is disabled; it supplies no CPU/memory observations. Historical image47/image48 tools remain unchanged. Worker-inclusive resource accounting, budget/contract crosswalk and broader milestone gates remain open.


[The bounded concurrent Source workflow](rust-m0-concurrent-bounded.md) now passes one real15/120s playback/import run: two imports, six actual task executions,44 durable checks, and physically settled bounded collector pipes. Four initial service censuses were complete with zero denials/races; the inherited merger omission is explicitly unqualified for resource use. The10s observer smoke occurs before workload only. It is not a worker-cost benchmark or M0 completion.
