# Actual parented Series and local VOD artwork reference

Pinned Original executed six real Celery tasks in five prefork workers across five phases: model-produced Refresh, its parented Series child, both real artwork children, a repeat Refresh, and its repeat Series child. Actual host and guest exits were zero; all 24 capture checks passed. The original Dispatcharr and dependency authors retain all application, algorithm and license credit.

The first Series created one Recording, a future clocked schedule and periodic task, then published two artwork children. Both inherited the Refresh root and Series parent. The first artwork task selected the owned local Movie's `vod_vodlogo`, saved poster/season/episode metadata, and emitted one recording_updated event. The second returned "ok" without another UPDATE/event. Repeat Series scheduled zero and published no artwork. Root, parent, backend parent-field presence, publication order, timestamps and SQL parameters are retained literally.

The owned XML/logo files are positive local fixtures. A dependency guard observed zero HTTP attempts; empty provider API keys alone would not prevent Source's keyless network fallbacks. No task/model/signal application methods were replaced. Three default descendants remain untouched in FIFO observation: EPG setup and two Index tasks. The future run_recording task was not executed. This establishes this local-logo/one-title Source domain, not default FIFO progress, general provider/artwork behavior, Native parity or full M6 acceptance. Source UID0 and /app/media are actual observations; packaged UID65532 has not been qualified here.

Default schema manifests before/after were byte-identical: 62 tables, 59 sequences, 501 columns, 198 constraints and 255 indexes. This is logical manifest equality, not database-page equality. All 44 host children/pipes settled; Root independently checked prior process identities and fresh owned-project plus both exact container inventories. No resource measurement occurred.

## Portable offline verification

From a published checkout:

```sh
python scripts/rust-job-contracts/parented-recordings-source/materialize.py --output /tmp/parented-source-evidence-NEW
python scripts/rust-job-contracts/parented-recordings-source/review.py --evidence /tmp/parented-source-evidence-NEW
python -m unittest discover -s scripts/rust-job-contracts/parented-recordings-source -p test_public.py
```

The evidence map binds 22 original files by uncompressed byte count/SHA and compressed SHA. Restoration is bounded, requires a fresh output, and starts no application. The source capture contains SQL instrumentation, not PG authentication wire. Its username/password fields are NULL; the actual fixture password label is the already-public `playback-fixture-only`. Privacy review scanned every restored JSON/log and 1,084 explicitly encoded fields without unpickling.

## Executed collector provenance and future reruns

`tools/` contains the eleven exact executed collector/support-map files. `original/` preserves eleven pinned Original modules with blob/SHA provenance. The `historical-*` preparation, case-plan, once-plan, environment and seal files preserve their actual paths/status; old preparation status strings are not new execution outcomes. Public offline tools above have actually run. Historical executor hashes and Root evidence bind the actual Source run.

A future application rerun is **not executed by these public verification commands**. It requires a fresh independently reviewed execution-input receipt, local actual image/daemon/Compose/tooling bindings, owned output and ordinary-fixture slot. There is no private-path fallback. Reconstruct the immutable support explicitly using the published helper:

```sh
python scripts/rust-integration/image54-checkpoint/reconstruct.py --kind support --repo "$PWD" --output /tmp/parented-support-f0-NEW
```

The reconstructed support must satisfy all 31 entries in `tools/support-inputs.json`. The host itself creates the immutable `git archive 1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and checks archive SHA `f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff`. Original image ID is `sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f`. Future clocks, PID/process identities and credentials must never be copied from the historical receipt to fake a replay.
