# M4 sustained playback experiment (preparation)

All application credit belongs to the original Dispatcharr authors and contributors. The original application, React interface, license and attribution remain the reference. These tools prepare a scoped measurement, not a claim of completed migration or performance gains.

`plan.py` freezes the workload and budgets before baseline measurements. `workload.py` keeps each viewer on one HTTP connection across the 15-second warmup and 60-second measurement. It records a bounded prefix hash, per-viewer delivered bytes, unexpected EOF, failures, first byte latency and whole service-group samples. It never retries or records credential-bearing endpoints. The initial profile is VOD and catch-up with 1 and 10 viewers, three alternating Source/candidate pairs per workload. A real WebSocket observer must run in each phase so lifecycle/statistics work is exercised.

The experiment requires the same immutable 900-second H264/AAC fixtures, database rows, provider capacity, backend CPU/memory limits and isolated PostgreSQL/Redis services for both implementations. Fixtures are generated once before the quiet window; no FFmpeg process runs during resource measurement. Warmup connections remain open during measurement. Each catch-up viewer has a separate user to avoid the original one-position-per-user displacement behavior. Between independent phases both backends stop, then only the disposable Redis database resets; the database fixture remains identical. Cold start, reconnect, slow readers and many-source channels are separate experiments.

The selected footprint includes infrastructure, origin and the active backend. The Source backend includes uWSGI and its actual Daphne observer endpoint. The candidate includes its HTTP/WebSocket gateway and actors. Docker working memory is the primary memory metric; summed process RSS is additional diagnostic evidence because PostgreSQL shared pages may be counted repeatedly. Inactive backends must be stopped and excluded; no unmeasured database or proxy is allowed. Planned cgroup limits are infrastructure 1CPU/1GiB, origin 2CPU/1GiB and backend 2CPU/512MiB, identical on each side.

All three pairs must pass payload hash, audio/video decoder, continuous-delivery, service-footprint and frozen resource budgets. Budgets are CPU ratio ≤0.80, maximum sampled working-memory ratio ≤0.95, throughput ratio ≥0.95 and first-byte ratio ≤1.10 with a 50ms comparison floor. Each viewer must deliver 90–110% of the declared paced origin rate, without EOF or errors. At least20 whole-group samples are required per phase. Medians are reported in addition to every pair; failed trials are retained and never replaced silently.

Before measurement, save the reviewed plan with:

```sh
python scripts/rust-m4-perf/plan.py --freeze artifacts/rust/m4-resource-plan.json
python -m unittest discover -s scripts/rust-m4-perf -p 'test_*.py'
```

Measurements remain blocked until Root coordinates a quiet window and supplies an explicit expiring authorization containing `scope: m4-resource-measurement`, `authorized: true`, the exact `plan_sha256`, immutable `image_id`, and timezone-aware `expires_utc`. Resource conclusions apply only to this workload. Full M4 parity remains open, including the documented original rapid-reclose duplicate-stop race.

`verified-preflight-image19.json` records the actual isolated Compose lifecycle and equal observed cgroups, deterministic long fixtures, bounded1MiB Source/native prefix hashes, actual WebSocket observers and strict actual HTTP audio/video decoders. Both backends decoded75VOD/74archive frames in3seconds without decoder stderr. The initial artificial truncated-prefix decoder failure is preserved. The MP4 is127,660,372bytes and MPEG-TS225,001,032bytes; exact generator commands and SHA256 values are recorded. No resource measurements ran in that preflight.

Prepare and decode a new immutable image (no resource samples):

```sh
python scripts/rust-m4-perf/run.py --output artifacts/rust/m4-preflight-NEW \
  --candidate-build-manifest artifacts/rust/CANDIDATE/build.json
```

Add `--load-smoke` for short sustained1/10-viewer correctness checks with no CPU/memory observers. `--measure --quiet-authorization PATH` runs the fixed full matrix only after explicit Root authorization. Four workloads ×three pairs ×two backends ×75seconds require30minutes plus setup/reset, so reserve roughly40minutes. The runner checks no unrelated containers remain running before each measured phase and revalidates the quiet authorization between phases. On interrupt it stops viewers and cleans up the uniquely named disposable Compose project.

Unit tests inject corrupt payloads, missing infrastructure, EOF, retries, inadequate throughput, nonfinite resource values, unequal observed cgroup limits, omitted/duplicated trials, post-hoc budget changes and stale authorization. A real loopback test verifies one request survives both phases; fake producer faults verify actual message-wrapper and Source success-field semantics. None is a performance acceptance result. The completed [image20 matrix](../../docs/rust-m4-performance.md) passes all twelve paired resource trials; this remains scoped playback evidence.

`verified-load-smoke-image19.json` now verifies all eight sustained Source/native protocol×1/10-viewer cases with one HTTP request per viewer, matching1MiB prefixes, zero EOF/errors, actual observer messages and scalar provider/metadata cleanup. Startup-probe retention is an original feature: initial archive captures and decoder reads must last beyond the two-second threshold before asserting established cleanup. The strengthened initial short-probe cleanup failure is preserved. The image20 resource matrix is documented separately below.

Concurrent original catch-up cleanup publications can arrive out of snapshot order: the10-viewer smoke ended with a previously computed count1 message despite all actual Redis metadata/counters being removed. Resource acceptance therefore requires an observed empty catch-up snapshot plus independent actual Redis cleanup, rather than asserting that the last delivered snapshot is the newest. No source publication-order correction is implied.

The completed matrix is in [verified-resources-image20-2.json](verified-resources-image20-2.json), including all numerical samples. Synthetic fixture tokens last two hours; production JWT settings remain unchanged. The runner rejects insufficient token lifetime before sampling and before each phase. The interrupted initial attempt is preserved without acceptance. Use `summarize.py --input RESULT --output JSON --markdown DOC` to revalidate all pairs and regenerate the scoped report.
