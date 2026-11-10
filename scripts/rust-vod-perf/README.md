# Ordinary VOD read experiment: image31 scoped result

All application credit belongs to original Dispatcharr authors/contributors. This separate experiment targets exact image31 only. All six predeclared pairs passed; numeric ratios/plots are published separately by the parent. No complete VOD/browser or wholeapp migration is claimed.

`protocol.py` declared six paired trials:1/10 virtual readers ×3 alternating trials. Readers use one Standard account and alternate ordinary Movie/Series list requests with explicit `ordering=name&page_size=24`, matching React's24-card setting. Source default20/cap100 remain unchanged; they are not retuned. Unique synthetic names avoid invented PK tie ordering.

The guarded original ORM fixture contains5000 movies,5000 series,50000 episodes,25 shared logos, three accounts (two active),4500 visible movies and series each, and duplicate active-provider links. All metadata is bounded synthetic data. Bulk creation avoids provider/job save signals. No provider refresh or media playback is invoked. Token minting occurs outside timing for each phase; original token lifetime is unchanged. Seed/phase fingerprints cover all fields of the owned Movie/Series/Episode/Logo/M3UAccount/threecontentrelation rows and CoreSettings. They exclude User, account profiles, categories and category relations. The schema hash covers selected information_schema.columns fields (table/column/type/nullability/default), not complete constraints/index DDL. Every measured response independently requires exact original raw bytes and parsed JSON.

The existing guide cgroup/observer code and finite service limits are reused: backend2CPU/512MiB, infrastructure1CPU/1GiB, origin2CPU/1GiB. Whole-group CPU and Docker working memory include infrastructure+origin+one backend; component results remain visible. ProcessRSS is diagnostic, never an aggregate memory headline.

After functional correctness and observer controls, Source-only pacing qualification tries10/5/2/1 seconds per request with5s warmup/15s observation. It selects the highest feasible predeclared rate using only Source metrics. Both backends use identical closed-loop endpoint alternation and staggering. The independent functional10-reader barrier remains unstaggered.

Measured phases use15s warmup and60s observation, minimum60 resource samples, zero errors/sample-errors/late-completed/OOM/counter-resets, exact raw body length/SHA and typed JSON for every response, unchanged fixture/schema and verified image/cgroup identities. Existing guide CPU≤1.0, memory≤1.0, completionp95≤1.10, TTFB≤1.10 with50ms floor, throughput≥.95 and pacing≥.95 budgets apply. Per-endpoint latency/throughput is retained and gated, not hidden by aggregate mixes. All six pairs must pass; medians cannot override a failed pair.

No workload can run merely by importing modules. Functional-only mode must not start a sampler. Qualification requires a fresh parent quiet cue bound to policy/manifest/boot; measurements require a separate cue bound to the frozen Source-selected plan. The captured run used separate fresh parent Source and measurement authorizations and passed original/native large-fixture checks first. The first attempt failed before observers/qualification because an inherited media-origin entrypoint exited; its cleanup/failure remains recorded. The successful retry used the existing guide idle-origin overlay and verified no origin requests. Policy/data/budgets were unchanged.


Successful evidence: `artifacts/rust/m5-vod-perf-experiment-image31-2`, with SourceObserver10samples/10seconds, eight Source-only qualification phases, six paired trials/twelve measured phases/720resource samples/3960verified responses, zero measured errors/late/OOM/resets, per-endpoint and aggregate gates, and owned Compose cleanup. `verified-experiment-image31-1.json` binds exact runtime/policy/plan/tools and scope. Parent independent review is `artifacts/rust/vod-quiet1-parent-final-review.json`.

`policy.json` retains its pre-declaration wrapper/status as historical evidence. The completed run status is in the verified summary; its policy digest remains unchanged. Normal functional mode starts no sampler. Reproducing qualification/measurement requires newly issued matching parent coordination files, never historical cues.

## Commands and published results

[Results, methodology and chart](../../docs/rust-vod-performance.md) accompany
[numerical evidence](evidence-experiment-image31-1.json) and
[the accepted resource report](verified-resources-image31-1.json).

```sh
python3 scripts/rust-vod-perf/run.py --action functional \
  --build-manifest artifacts/rust/candidate-build-vod-reads-image31-1/build.json \
  --output artifacts/rust/vod-functional-new-empty-directory

python3 scripts/rust-vod-perf/run.py --action experiment \
  --build-manifest artifacts/rust/candidate-build-vod-reads-image31-1/build.json \
  --quiet-authorization artifacts/rust/fresh-vod-source-cue.json \
  --measurement-authorization artifacts/rust/future-vod-measurement-cue.json \
  --output artifacts/rust/vod-experiment-new-empty-directory

python3 scripts/rust-vod-perf/publish.py \
  --evidence artifacts/rust/m5-vod-perf-experiment-image31-2 \
  --build-manifest artifacts/rust/candidate-build-vod-reads-image31-1/build.json \
  --output artifacts/rust/vod-publication-new-empty-directory

python3 scripts/rust-vod-perf/plot.py \
  --report scripts/rust-vod-perf/verified-resources-image31-1.json \
  --output-prefix artifacts/rust/vod-list-performance
```

The coordinator pauses competing work before issuing the Source cue. After the
runner writes `experiment/frozen-plan.json`, it waits for a separate measurement
cue matching that plan. `protocol.authorize` defines the required scope,
candidate/manifest/policy hashes, boot ID and future expiry. Do not copy old
authorizations. A rebuild with a different image ID needs its own policy declared
before observations; the historical image31 policy intentionally rejects it.
Rendering the chart requires Matplotlib. Publication rechecks all six pairs,
endpoint data, sample counts and group resource totals, and rejects incomplete
or internally inconsistent evidence.
