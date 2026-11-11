# Same-runtime authenticated live churn

Original Dispatcharr authors and contributors retain all application/algorithm
credit and original licenses.

[The image47 recording](projection.json) passes12 rounds, six per backend,
with66 authenticated readers and66 bounded2MiB audio/video decodes. Each backend
uses one unchanged container for the fixed schedule1,10,1,10,1,10 viewers. Its
provider has a finite one-stream limit. Every round verifies the real user ID
in client metadata and returns owner, provider-slot, client and metadata counts
to zero, with no active upstream afterward. Docker IDs/start times remain
unchanged between rounds; no Redis reset or backend restart occurs there.

One three-second anonymous decoder primer runs only at each backend's startup.
There are no primers between rounds or retries replacing failed requests. Before
native execution, a separate actual Original-only qualification passed all six
rounds/33 readers using identical critical controller/workload/identity/protocol
bytes. A full later Source/native run then passed all12 rounds. Backend switching
uses a reset between the independent backends; this is not a cross-backend
continuity test.

The parent independently verifies all66 retained media/transcript hashes, frame
counts, identities, unchanged runtimes and zero post-round counts. Both owned
projects have no remaining containers (including stopped ones), networks or
volumes. Raw media remains local; hashes and observations are published.

No resource sampler runs. This is functional churn evidence, not CPU/memory,
fd/thread growth, long-term leak, every-byte decode, full lifecycle or M2/M0
completion. Prefix decoding permits initial GOP recovery and arbitrary cutoff
tails; it uses fixed five-second output and at least25 video frames. Earlier
performance results retain their own separate image/workload bindings.

## Verify and repeat

Numerical validation and ten actual-record corruption controls need only Python:

```sh
python3 scripts/rust-live-perf/churn/verify.py \
  --projection scripts/rust-live-perf/churn/projection.json
python3 -m unittest discover -s scripts/rust-live-perf/churn -p test_verify.py -v
```

Use a clean checkout, Docker Compose and an immutable passing app build manifest
for actual execution:

```sh
python3 scripts/rust-live-perf/churn/run.py --repo . --source-only \
  --build-manifest artifacts/rust/YOUR_BUILD/build.json \
  --output artifacts/rust/live-churn-Source-NEW
python3 scripts/rust-live-perf/churn/run.py --repo . \
  --build-manifest artifacts/rust/YOUR_BUILD/build.json \
  --output artifacts/rust/live-churn-paired-NEW
```

The executed support revision is recorded in the projection. The controller
archives committed support plus its exact own tools, verifies the unchanged
pinned Original archive/image, authenticates using an in-memory fixture token,
checks the candidate image/source labels and destroys its owned synthetic
Compose fixture. The output directory must be new. The schedule is fixed by
`protocol.json`; inherited `--viewers` and `--quiet-cue` arguments do not select
another scenario. Only functional action is exposed. Inherited preparatory
resource-status fields are not acceptance results; the report explicitly keeps
`performance_verified: false`.

[Image47 application reconstruction](../image47/README.md) records exact app
inputs. A rebuilt/different candidate must earn its own functional evidence;
the public verifier deliberately binds this dataset to the actual image47 ID.
