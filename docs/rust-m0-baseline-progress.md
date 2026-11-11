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
| Idle before/after playback and reconnect churn | Execute the sealed three-repetition Source idle/census/growth policy under an otherwise idle host. The observer smoke alone is not acceptance. |
| 1/10/100 viewers on one channel | Reconcile existing repeated warmed measurements with their exact budgets and preconditions; retain the cold100 failure separately. |
| Ten distinct channels and configured capacity | Add explicit finite-capacity accounting to the existing many-channel workload. One shared provider slot on one channel is a different case. |
| Slow plus fast readers | Add missing Source resource and post-close accounting to the existing9-fast/1-slow workload. |
| Stall, failure, stream change and recovery | Attach resource, valid-work and cleanup accounting to the existing fault/switch fixture. Keep the interior failover decode warning and later recovery separate. |
| Import/EPG refresh during playback | Capture one real owned Source import/worker and playback workload concurrently, including worker cost and durable effects. Separate successful tests do not establish contention behavior. |
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
