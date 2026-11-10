# VOD category metadata performance

Original Dispatcharr authors and contributors retain all application and algorithm credit. AGPL licensing is retained.

The canonical image34 experiment passed all six paired trials. For this fixed category JSON endpoint, median paired CPU fell 70.4%/70.7%, sampled peak service-group working memory fell 60.4%/61.6%, and completion p95 fell 65.0%/65.7% for one/ten readers. Throughput was unchanged. This measures category metadata reads, not complete VOD or application performance.

The [scoped report](../../docs/rust-vod-category-performance.md), [verified summary](verified-experiment-image34-1.json) and [numerical samples/timings](evidence-experiment-image34-1.json) retain the immutable policy, image and Source-selected plan bindings. The following protocol notes describe the declaration and execution boundaries used for this run.

# Category metadata experiment: declared draft

All application credit belongs to the original Dispatcharr authors and contributors; AGPL attribution is retained. This is a separate experiment from ordinary VOD image31 and category image33. No resource observations or gains are claimed here.

`policy.json` declares the resource and correctness budgets now. The base spec keeps an empty image binding; a separate immutable bound-policy record now targets canonical image34 after its scoped functional review. The final bound policy digest must be recorded before any candidate resource value is observed. No fixture size, query, service limit, CPU/memory/latency threshold or rate candidates may be tuned from candidate results. A later identity is a new experiment, not a relabeling of an earlier result.

The workload reads one unpaginated category endpoint with `ordering=category_type,name`. The 240 synthetic names are unique within each type; the two global Uncategorized rows differ by type, so the explicit supported ordering has a unique tuple without inventing a PK tie breaker. The response contains 242 categories and 501 nested provider relations, including disabled links, inactive XC and STD metadata. Three active XC accounts have their six global default links before timing. Every request still executes the original initialization/existence checks. First-creation and injected error behavior belong to the separate functional contracts.

The five owned accounts are three active XC, one inactive XC and one active STD. Two cyclic XC links per ordinary category produce 480 rows; every sixteenth category also has a STD link (15), plus six global links. Properties remain bounded synthetic Unicode/JSON data. One active Standard account enables both content types. Movie/Series/Episode/logo tables must be empty. No provider or asset is contacted; an idle owned origin must report zero requests.

The existing VOD experiment framework supplies the unchanged guide observer, cgroup counters, Docker sampler, closed-loop scheduling and acceptance calculations. Selected service groups include infrastructure, idle origin and exactly one backend. Backend limits remain 2 CPUs/512 MiB, infrastructure 1 CPU/1 GiB, origin 2 CPUs/1 GiB. CPU and Docker working memory are whole-group headlines, with components retained; summed process RSS is not a headline.

Functional verification comes first: exact Original/native raw JSON, status and MIME; all ordered IDs/names/types/properties and all 501 nested rows; real unstaggered ten-reader correctness; input/effect/schema/identity guards; and cleanup. It starts no observer or sampler. Every measured response repeats exact raw-byte and typed JSON validation. Fingerprints cover the explicitly listed user/provider/profile/category/relation/settings rows and verify content-table emptiness, rather than claiming unspecified full-database immutability.

Only a fresh parent quiet cue permits Source observer smoke and qualification. Source tries periods 10/5/2/1 seconds for one and ten virtual readers, with 5 s warmup/15 s observation. The highest feasible fixed rate is selected using only Source metrics. After that plan is frozen, measurements require another parent cue. Three alternating pairs per workload use 15 s warmup/60 s measurement: six pairs/twelve phases. Competing builds, Docker fixtures, media jobs and tests must be stopped before qualification and measurement; code review alone can continue.

Budgets retain the earlier methodology: native/source CPU and Docker working-memory ratios at most 1.0; completion p95 at most 1.10; TTFB p95 at most 1.10 with the existing 50 ms floor; throughput/pacing at least 0.95. Every measured phase needs at least 60 samples and zero response errors, wrong bodies, sample errors, late completions, OOM kills or counter resets. Every pair must pass; a median cannot erase a failure. Failed attempts remain preserved and stop acceptance. No claims extend to browser viewing, playback, provider refresh, other API routes or the whole application.

At declaration time, candidate functional validation passed before observer/qualification/measurement authorization. The completed run and separate parent authorizations are now documented above. The initial deliverables are the policy, guarded fixture, exact reader and controls plan. Seven pure controls now pass. Source-only and Source/native functional validation have executed; at that checkpoint no observer, qualification, candidate resource or measurement run had executed. Runner adaptation and fresh-clone execution will be reviewed separately; importing these modules must never launch a workload.

## Execution boundaries

The runner now implements Source-only functional mode. It archives the pinned original source, freezes its tools/spec, creates only the Original infrastructure/idle-origin/Source services, verifies declared limits and zero origin requests, captures the exact category body, runs one/ten-reader barriers, rechecks fingerprints, and removes its owned Compose project. It does not import the observer or orchestrator measurement branch. The first such run passed with a 97,835-byte response; it is functional evidence, not pacing or resource qualification.

```sh
python3 scripts/rust-vod-category-perf/run.py --action source-functional \
  --output artifacts/rust/new-category-source-functional
```

The following command templates require a passing canonical manifest and exact full Git revision; image34 is now bound as recorded below. Binding writes a new file exclusively; it does not authorize sampling. Functional Source/native comparison remains separate from an experiment.

```sh
python3 scripts/rust-vod-category-perf/run.py --action bind \
  --build-manifest artifacts/rust/future-image34/build.json \
  --revision FULL_IMAGE34_REVISION \
  --output artifacts/rust/new-category-policy-binding

python3 scripts/rust-vod-category-perf/run.py --action functional \
  --build-manifest artifacts/rust/future-image34/build.json \
  --policy-record artifacts/rust/new-category-policy-binding/bound-policy.json \
  --output artifacts/rust/new-category-source-native-functional

python3 scripts/rust-vod-category-perf/run.py --action experiment \
  --build-manifest artifacts/rust/future-image34/build.json \
  --policy-record artifacts/rust/new-category-policy-binding/bound-policy.json \
  --quiet-authorization artifacts/rust/fresh-category-source-cue.json \
  --measurement-authorization artifacts/rust/future-category-measurement-cue.json \
  --output artifacts/rust/new-category-experiment
```

The experiment adapter uses one endpoint and category-specific bound-policy/plan checks. Source observer smoke and qualification cannot start without the first cue; the native measurement loop waits for the separate plan-bound cue. The existing observer/sampler modules remain unchanged. The first experiment subsequently completed under separate Source and plan-bound measurement cues; its exact outcome is linked above.

Current functional evidence is `verified-functional-image34-1.json` (same 97,835-byte Original/native body under one/ten barrier readers, unchanged fingerprints/limits/zero-origin checks). The exact bound-policy record is `artifacts/rust/category-perf-policy-image34-1/bound-policy.json`, policy SHA a069f665602ad1565ebc9c7ddb10d63cbd2da51a6c07474223630f7ec331dcb3. This is a declaration and functional result, not permission to sample. Every new experiment still requires a new Root review and category-specific quiet authorization before Source observer smoke/qualification, followed by a separate plan-bound measurement cue.
