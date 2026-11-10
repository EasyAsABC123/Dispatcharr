# Unified Both/All VOD read experiment

Original Dispatcharr authors and contributors retain full application and algorithm credit; AGPL licensing remains in force. This separate image36 experiment reuses the committed ordinary-list observer, fixture and trial method. Historical image31/image34 policies and results remain unchanged. No resource gains are accepted by this declaration.

The fixed endpoint is `/api/vod/all/?page_size=24`. The fixture has 5,000 movies, 5,000 series, 50,000 episodes and 25 logos, with 4,500 visible entries of each content kind. Every response must match the actual Original raw JSON bytes, ordered rows and metadata exactly, with count 9,000 and 24 results. Names are unique across kinds, avoiding tied ordering. One Standard account is shared across readers. No provider refresh or playback is invoked.

Functional mode runs unstaggered one/ten-reader comparisons, verifies immutable fixture/schema fingerprints and candidate identity, and starts no sampler. Source-only qualification then chooses among predeclared 10/5/2/1-second periods. Measurements use three alternating pairs for each one/ten-reader workload, 15-second warmup and 60-second observation. All six pairs must pass CPU, whole-service-group working memory, latency, throughput and error gates. The complete predeclared limits and budgets are in `policy.json`. Fixture fingerprints have the same explicitly limited table/column coverage as the historical ordinary-list experiment; they do not establish complete schema/constraint parity.

Qualification and measurement each require a separate fresh parent coordination cue bound to the exact image, policy, boot and frozen Source-selected plan. Parent coordination must pause competing workloads first. Functional verification can run during development; no measurement or gain claim follows.

```sh
python3 scripts/rust-unified-perf/run.py --action functional \
  --build-manifest artifacts/rust/candidate-build-unified-recovery-jobs-image36-1/build.json \
  --output artifacts/rust/unified-functional-NEW
```

The first qualification attempt completed its eight Original-only phases, but parent review refused measurement authorization after finding a remaining two-endpoint comparison loop. No paired measurement started; the owned project was removed. The corrected comparison derives its exact endpoint set from the declared policy and rejects missing or extra endpoints. Five controller/reader safety controls pass. A new attempt must repeat qualification with fresh cues; policy, fixture, pacing choices and budgets are unchanged.
