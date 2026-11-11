# Actual shared-buffer core comparison

This tooling compares actual Original StreamBuffer/Redis with native packet assembly, InputStore, Channel and History. It measures one active core plus common PG/Redis infrastructure. Original web and origin are stopped. Both core runtimes use the identical pinned Original image as their runtime/ELF loader. Native executes a standalone optimized Rust test ELF; no application image or image44 performance is transferred to this result. Original Dispatcharr authors retain application/algorithm credit and original licenses.

The retained experiment2 passes its fixed comparative benchmark gate: nine Source-only qualifications, then three alternating paired trials at each1/10/100 readers. Each phase has15seconds warmup and60seconds measurement. The common250ms producer period was selected solely from all nine Source qualifications before native measurements. Source and native use188-byte packets,1361packets/255868bytes per chunk,8192-byte input frames,60-second retention; native authoritative History is128MiB. Slow-reader cohorts exercise actual History. Both the core and infrastructure receive2CPU/512MiB limits. Final full bytes and sequence agree, every measured reader-class ratio is1.0, all1620 periodic samples are retained, all absolute lifecycle OOM counters are zero, owned cleanup completes and the default fixture DB is unchanged.

| Readers | Median paired CPU reduction | Median paired peak working-memory reduction |
|---:|---:|---:|
|1|26.1456%|29.4209%|
|10|73.0244%|32.0612%|
|100|91.1180%|48.6231%|

Each reduction is100×(1−the median native/Original ratio) across three pairs in that cohort. Every individual pair passes CPU<=0.90, peak working memory<=0.95 and measured per-class work>=0.98. Absolute values, raw numeric traces/counters and individual ratios are in the independently validated projection. This does not close all M1 milestone requirements or establish application/HTTP performance. The milestone ledger lists remaining acceptance gaps. The older TS-helper microbenchmark is a separate workload.

## Reproduce exact source and optimized ELF

From the repository root:

```sh
python scripts/rust-core-perf/reconstruct.py --output /tmp/m1-exact-source
cd /tmp/m1-exact-source/rust
cargo test --release --locked --offline -p dispatcharr-stream-http --lib --no-run --message-format=json
```

The reconstruction requires locally available Git revision24d9e72b64df1afe871f935826824a4c96fd6ade and a new empty destination. Five exact overlays are included: InputStore source-era lib, its source_expiry test, the corresponding Main configuration, engine cfg(test) declaration and core_perf_peer. The entire reconstructed source map must match`dd3e02227a38f595ace68cfa89f5414bb24efd55f87e6c3f072452280319fd25`; current canonical code is not substituted. Use pinned Rust1.90. Retain compiler artifact metadata and identify the resulting library-test ELF; do not choose an arbitrary target filename. Measured ELF SHA256 is`2a840aee2cad30e7716abe1dc9add51bd9862df70a89ed78ab24ff9541738a9a`, opt-level3/debug0/debug-assertionsfalse/overflow-checksfalse/testtrue. A changed rebuilt hash remains a separate reproduction artifact until explained.

This package embeds the complete pinned Original1211-file source hash map. The controller obtains the exact Original Git archive1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430 and requires its archive SHA256`f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff`, then verifies every extracted source file before Python executes. All31 bundled support files, including transitive Python imports and fixture scripts, are verified before helper import; the13 measured critical support files remain byte-identical. There is no private SUPPORT-directory fallback.

## Functional replay and new experiments

Run from the repository root, with Docker configured for the disposable fixture:

```sh
python scripts/rust-core-perf/tools/controller.py --action functional \
  --binary /path/to/verified-native-core-peer --output /tmp/m1-new-functional
```

The functional replay produces44chunks at1second warmup/10seconds nominal work for each actual Source/native1/10/100 cohort; every reader receives11,258,192bytes. Held cached/History payload controls check one-frame credit and final ownership. This action starts no resource sampler. The wrapper records the worker's real exit code and remains alive to retain its cgroup; post-window draining waits for a shared barrier released after final counters in experiments. The controller uses guarded copied fixture databases and internal networking and removes its project/volumes.

A new resource experiment additionally requires fresh Source and measurement quiet-cue paths and the exact prior functional proof. Source qualification completes and seals before the separately issued measurement cue. Never reuse historical cues, tune thresholds or transfer pacing based on native results. The measured controller tools are retained separately in experiment2 raw evidence; the portable path adapter has its own hashes and functional replay and is not falsely represented as the exact historical tool bytes. Experiment1's prequalification import collision and portable1's sandbox socket refusal remain preserved failed attempts.

## Validate and publish retained evidence

```sh
python scripts/rust-core-perf/publish.py --input /path/to/retained-experiment2 --output /tmp/core-projection.json
M1_RETAINED_EXPERIMENT=/path/to/retained-experiment2 python -m unittest discover -s scripts/rust-core-perf -p 'test_*.py'
python -m unittest discover -s scripts/rust-core-perf/tools -p 'test_*.py'
python scripts/rust-core-perf/plots.py --projection /tmp/core-projection.json --output /tmp/core-plots
```

Publisher recomputes actual worker work, service cgroup deltas, Docker working-set memory, sample boundaries/cadence, Source qualification/common pacing, sealed bindings, all paired budgets and absolute lifecycle OOM/exit/cleanup. Negative controls corrupt actual retained evidence independently (missing/reordered phase, seal, qualification, cleanup, bytes, OOM, reset, working-set formula, cadence, sample window, exit, pacing and normalized metrics). Numeric projection excludes credentials, runtime container IDs and task/channel UUIDs while preserving labeled counters, lifecycle state, reader work and numeric samples. Raw private evidence and failed attempts remain unchanged; projection includes exact raw-file hashes. Plots require matplotlib; controllers/publisher use Python's standard library. They show paired ratios and cohort medians only.

To run a new resource experiment after its functional proof, use
`--action experiment --binary YOUR_ELF --output NEW_EMPTY_OUTPUT` with
`--functional-proof FUNCTIONAL/functional.json`, `--source-quiet SOURCE_CUE.json`
and `--measurement-quiet MEASUREMENT_CUE.json`. Quiet cues coordinate an already
authorized local experiment; they do not require another user approval.
Their fields are `authorized:true`, matching `policy_sha256`, the actual
`native_binary_sha256`, current `/proc/sys/kernel/random/boot_id`, and an aware
future `expires_utc`. The Source scope is `m1-core-source-qualification`. Create
the measurement cue only after all Source qualifications produce the immutable
`frozen-plan.json`; its scope is `m1-core-resource-measurement` and it must also
name that file's `plan_sha256`. Keep builds, unrelated fixtures and other resource
samplers stopped throughout. A failed gate retains evidence and ends the run.
