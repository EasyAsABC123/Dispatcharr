# Stored React guide GRID experiment

Original Dispatcharr authors and contributors retain all application and algorithm credit and AGPL licensing.

This is a separate experiment from the accepted Current/XMLTV workload. `grid_plan.py` now pins image30's exact image ID, source tree and archive; frozen image29 tools remain in their retained failure artifacts. Its CPU, memory, TTFB, completion, throughput, correctness and sampling budgets match the existing guide policy. The default `plan.py` is unchanged. The same service limits, whole selected service group, observer, 15-second warmup and 60-second measurement apply.

The fixture has 500 channels sharing 50 real EPG entries and 2,250 stored programmes. GRID-only seeding fills each programme's `tvg_id` from its EPG entry; the existing Current/XMLTV fixture branch is unchanged. The explicit window is seed clock minus one hour through plus 24 hours. Every response must match the original typed JSON and array order, with `data` containing 2,250 distinct programme IDs and 50 TVG identifiers. No XML request is sent by the GRID functional or qualification/matrix path.

The two workloads use one and ten viewers. Original-only qualification considers periods 10, 5, 2 and 1 seconds before selecting rates. The six required pairs use Source/candidate, candidate/Source, Source/candidate order for each workload. Candidate CPU cannot select a rate. The shared orchestrator receives separate policy, qualification and acceptance modules per invocation; it does not mutate the default modules.

Policy records `artifacts/rust/grid-resource-policy-image29-{1,2}.json` were written before any native resource observations. Policy2 corrects only the Source fixture's absent stored TVG links. It preserves every budget and image identity. The first Source-only assertion failure is retained.

```sh
python scripts/rust-guide-perf/smoke.py --workload-family grid --action functional \
  --candidate-build-manifest artifacts/rust/candidate-build-grid-queue-image30-1/build.json \
  --output artifacts/rust/grid-functional-new-empty-directory
```

Image29 fails the unchanged ten-viewer simultaneous startup check: the original returns ten matching HTTP200 responses; native returns two matching HTTP200 responses and eight HTTP503 JSON objects with a `detail` key. Attempts2–4 preserve that mismatch. The final diagnostic reads at most 4,097 error bytes and retains status/MIME/body class/key names, never body values or authorization. See `verified-grid-functional-image29-4.json`. This is a visible admission mismatch; neither raising limits nor staggering the functional burst accepts it. A bounded queued-admission fix and a new immutable candidate must pass the same check before measurements.

The complete frozen pure-tool closure at `artifacts/rust/grid-controls-30` passes 30 controls, including the default fifteen-pair path and a separate eight-rate-check/six-pair GRID path that rejects any XML/cache access. One bounded historical publication-control input is included so these frozen controls run without skips.

A future `--action experiment` needs two separately issued parent quiet cues. Their scopes are `grid-source-qualification` and `grid-resource-measurement`; they bind the policy digest, exact candidate image, host boot and expiry. Qualification emits a frozen plan and waits for the measurement cue. The completed image30 qualification and six-pair matrix are recorded below. A replacement image requires a newly declared identity/policy record before native resource observations. Functional correctness does not establish performance or full migration acceptance.


Image30 passes the identical functional burst: Source10/10 and native10/10 return HTTP200 with exact typed/order programme JSON. `grid-resource-policy-image30-1.json` changes only candidate image/tree identity fields from policy2; its explicit delta proves the fixture, workloads, service limits, qualification and every budget are unchanged. Native resource values were not consulted. See `verified-grid-functional-image30-1.json`. This closes the witnessed startup-admission mismatch for this workload, without accepting production-wide concurrency, migration or performance. Resource qualification and all six pairs subsequently passed under the recorded parent coordination.

## Completed image30 experiment

[The accepted report](verified-grid-experiment-image30-1.json) and [numerical evidence](evidence-grid-experiment-image30-1.json) retain all 20 phases, 840 resource samples and 2,445 timing rows. Every measured phase has 60 samples and no request errors, late completions, OOM events or counter resets. Source-only qualification selected one second for one viewer and two seconds for ten viewers; the rejected ten-viewer one-second rate and ten late qualification completions remain recorded.

Across the three pairs for each workload, median paired CPU decreases are 71.6% and 73.3%; sampled peak group working-memory decreases are 60.0% and 59.6%. Throughput ratios are both 1.0. [The methodology and plots](../../docs/rust-guide-performance.md#stored-react-guide-grid--image30) describe limits and scope. These results bind image30 only. Later application builds require their own correctness and performance evidence.
