# Repeatable playback performance checks

## Validate the harness with Docker Compose

```sh
python3 scripts/rust-perf/selftest.py --output-dir /tmp/dispatcharr-perf-selftest
```

This builds a pinned Python fixture image, publishes a deterministic paced byte
stream on loopback port 19292, verifies its Compose service label, runs two
10-second two-viewer workloads, samples container CPU and memory, and checks
request/byte/disconnect counters after cleanup. It tests an identity comparison
and rejection of an injected load error. The workloads are synthetic bytes, not
video; this verifies the harness and makes **no backend performance claim**.
The fixture stack is removed in a finally block. Set `PERF_FIXTURE_PORT` to change
the published port or `--project` to isolate another run. `--keep-running` retains
the stack for inspection. Docker socket access and local sockets must be allowed.
`selftest-evidence.json` records the initial successful isolated Docker run,
including counters, sample counts, and workload parameters. It measures only this
synthetic fixture and must not be used as evidence of Python-to-Rust gains.

`perf.py` uses Python's standard library and Docker Compose v2. It applies bounded
HTTP load to **already configured local test services** and samples the sum of
Docker CPU percentages and memory usage across every selected service container.
Include Redis, database, background workers, and media sidecars as well as the web
service. FFmpeg children inside a selected container are included automatically.
Do not compare a full Python stack against only a Rust web container.

This is an integration load harness, not a playback correctness test. It reads
continuous TS/fMP4 or VOD bodies, checks that every viewer receives bytes, and
records first-byte latency. It does not decode video, follow HLS playlists, detect
TS discontinuities, or verify audio. Use decoder and contract checks from the
milestone plan alongside it. Do not interpret a buffer microbenchmark as full
application savings.

## Run

Use deterministic local media, matching profiles and restored fixture databases.
Avoid production provider URLs: the harness opens real viewing sessions and may
reconnect when a response ends. Do not run baseline and candidate simultaneously;
other host load changes invalidate comparisons. Warmup opens streams, closes them,
then starts fresh measured connections; it warms caches but includes measured
connection startup. Request timeout bounds stalled reads and can add up to that
timeout to wall-clock completion. Bytes read after the measurement deadline and
resource samples completed after it are excluded; throughput divides only bytes
received within the requested measurement period by that period. Streams should
emit regularly within the timeout.

Create a file outside git containing one playback URL per line. Keep credentials
out of shell command arguments. JSON output omits URLs and exception text.

```sh
python3 scripts/rust-perf/perf.py run \
  --compose-file docker/docker-compose.yml --project dispatcharr-baseline \
  --services web celery db redis \
  --urls-file /tmp/baseline-playback-urls.txt --scenario same-channel \
  --viewers 10 --warmup 15 --duration 60 \
  --revision EXACT_BASELINE_IMAGE_DIGEST --fixture-id FIXTURE_SHA256_PROFILE_COPY \
  --output /tmp/perf/baseline-10.json
```

Service names are examples: pass the actual complete service list from the chosen
Compose file (`docker compose -f FILE config --services`). The command fails if
any selected service is not running. Repeat for the candidate with matching
fixture content, URL count, viewers, warmup, duration, timeout, and slow delay.
Use `--scenario many-channel` with a distinct fixture channel URL per viewer.
Use `--slow-delay 0.25` to pause between reads for a slow-consumer scenario.
Run 1, 10 and 100 viewers; perform at least three paired trials per scenario,
alternating baseline/candidate order. Keep JSON artifacts with the milestone.
Increase duration to at least 30 minutes for leak/retention observation.

```sh
python3 scripts/rust-perf/perf.py compare \
  /tmp/perf/baseline-10.json /tmp/perf/candidate-10.json \
  --max-cpu-ratio 0.8 --max-memory-ratio 0.8 --max-latency-ratio 1.10
python3 -m unittest discover -s scripts/rust-perf -v
```

Ratios are candidate/baseline. The example requires 20% lower mean CPU and peak
memory with at most 10% greater median first-byte latency. These are configurable
example gates, not an assertion that those improvements have been achieved.
The default comparison gates allow no CPU/memory regression. Both runs must have
at least three resource samples, no HTTP/sampling errors, no empty responses, and
bytes for every viewer. Candidate throughput must reach 95% of baseline to avoid
passing by delivering less video. Verify media equivalence separately: the harness
cannot prove that arbitrary different URLs represent identical source fixtures.

Docker CPU percent uses 100% per CPU core and is summed across services; it is not
CPU seconds. Docker memory may subtract filesystem cache depending on platform;
compare on the same Docker host/version. This tool does not currently capture
per-process PSS, Redis command counts, GPU load, or cleanup after disconnect. Those
require separate milestone probes. Very short trials and low CPU baselines produce
unstable percentage ratios; inspect raw samples, use longer paired trials, and
set gates before evaluating candidate results.

## Frozen repeated-trial resource gate

`milestone.py` accepts only the strict `docker-http-resource-harness` budget
schema illustrated in `docs/rust-harness-budgets.example.json`. Copy the unfrozen
template, fill every field, and freeze it before candidate evaluation. It rejects
the separate full-service budget: cleanup, decoded-media validity, CPU seconds,
and full-milestone acceptance are outside this tool's scope.

Create an evidence manifest with every frozen scenario and at least three
independent baseline/candidate report pairs. Artifact paths are relative to the
manifest file:

```json
{"scenarios":[{"id":"same-channel-10-viewers-ts-copy","pairs":[
  {"baseline":"old-1.json","candidate":"new-1.json"},
  {"baseline":"old-2.json","candidate":"new-2.json"},
  {"baseline":"old-3.json","candidate":"new-3.json"}
]}]}
```

```sh
python3 scripts/rust-perf/milestone.py --budget /tmp/frozen-harness-budget.json \
  --evidence /tmp/perf/pairs.json --output /tmp/perf/resource-gate.json
```

Every pair must pass; there is no median aggregation that can hide a failed
trial. Unknown/null fields, unfrozen budgets, absent scenarios, reused report
contents, wrong environment/revision/fixture/workload/service footprints, errors,
insufficient samples, missing output, low absolute throughput, excessive p95
first-byte latency, and CPU/memory ratio failures reject the gate. Revisions and
fixture identifiers in reports are operator-supplied assertions: independently
retain image/config/fixture provenance and correctness evidence. Report hashes
prevent accidental reuse, not deliberate fabrication or proof of independence.

`perf.py` now records nearest-rank first-byte p95 and its sample count. With only
one connection per viewer, that count may be small; declare a meaningful
`minimum_first_byte_samples` before running (20 or more when feasible) and capture
enough connection startups. First byte is not first decodable frame. The report
always states `application_performance_verified:false`, even when this narrower
resource gate passes. The existing single-pair CLI comparison also enforces a
95% throughput floor, so frozen budgets cannot choose a lower floor.
