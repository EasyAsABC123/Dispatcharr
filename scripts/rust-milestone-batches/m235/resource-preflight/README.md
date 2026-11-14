# Resource preflight: first measured phase failed qualification

All application credit belongs to Dispatcharr’s original authors and contributors.

This preserves a failed diagnostic, not a completed four-phase benchmark. The
predeclared workload/order was Original, Native, Native, Original, with fresh
fixtures and a30-second resource window per phase. A parser-only first attempt
failed before any application or measurement; its evidence and executed drivers
are retained separately. The corrected attempt reached31 Original samples over
29.999seconds and then stopped with actual exit1. No Native or fourth Original
measurement ran, and no ratios or gains were accepted.

Original delivered HTTP200 MPEG-TS with zero packet sync errors, no reader errors,
and two active clients sharing one slot/upstream throughout the window. Both
readers advanced less than the frozen5,000,000byte minimum:4,605,624 and4,861,492.
At13 sampled ticks, at least one viewer had not advanced since the previous tick.
These fail the exact predeclared rule. Buffered/chunked delivery is an observation,
not a reason to retrospectively relax the rule. This does not undo the separate
six-sample Source decoder authority. This diagnostic’s off-window decoder was
not run because its useful-work failure preceded that code path.

The raw resource data includes all three owned service cgroups (Original backend
and infrastructure, replica, deterministic origin) and both dedicated observer
scope cgroups. Charged memory, working-set estimate, raw CPU and monotonic times
are distinct; whole-fixture memory is aggregated per sample, not summed peaks.
Single invalid-phase resource observations are retained for diagnosis only.

Teardown passed. Eight fresh independent checks confirmed project container,
network and volume inventories empty; all three exact containers absent; and both
observer scopes inactive, not found and without a control group. The global lease
was released. There is no CPU/memory performance gain or full M3 acceptance.

A future experiment needs a separately reviewed frozen usefulness criterion that
accounts for observed delivery cadence and actual fixture bitrate. No new run,
retry or gate change is authorized by this publication.
