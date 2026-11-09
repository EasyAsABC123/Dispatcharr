# Shared native TS worker contracts

Full original application credit belongs to Dispatcharr's original authors and
contributors; see [ATTRIBUTION.md](../../ATTRIBUTION.md). Original source and
license remain intact. The runner uses an unchanged `git archive` of the pinned
baseline, two original Python web workers or two identically source-bound Rust
containers, and the same isolated PostgreSQL/Redis/origin fixtures.

```sh
python scripts/rust-integration/shared_ts.py \
  --candidate-image UNIQUE-TAG \
  --candidate-build-manifest artifacts/rust/BUILD/build.json \
  --output artifacts/rust/NEW-SHARED-TS-RUN
```

`--backends baseline` observes the original two-worker contracts independently.
`--backends candidate` tests the candidate only; `--cases shared-demand stop`
selects a subset. Hard owner crash must be last. Cases verify one upstream and
one provider reservation with the real fixture provider limited to one stream,
real media on both workers, continued follower media after the owner's local
viewer disconnects, follower-issued global stop, hard owner crash, metadata and
slot cleanup, packet synchronization and independent FFmpeg video/audio decode.
Readers consume incrementally, preserve bounded captures, and close their own
sockets. Crash kills only the owned web process group or candidate container,
leaving the shared fixture PostgreSQL and Redis alive.

Original crash recovery is measured separately from healthy protocol contracts.
The first baseline run showed the existing leaked provider count/index after
ninety seconds (`m2-shared-ts-baseline-1`). The runner records that failed resource
gate and continues candidate testing; `passed` stays false when any resource gate
fails. Separate `baseline_protocol_functional_passed` and
`candidate_functional_and_recovery_passed` fields prevent hiding the baseline
leak or mislabeling a candidate recovery improvement as identical crash behavior.
No automatic takeover is invented: the original source explicitly describes
zombie takeover as future work, and the probe verifies no second origin opens.

Candidate manifests must match their canonical source hash, container image IDs
and image source labels on both replicas. Baseline archive and active tool hashes
are captured. Changing tested tools during execution invalidates the run. All
resources belong to a uniquely named Compose project; cleanup touches only that
project. If teardown fails, source mounts and resource identifiers are retained
for review. These are functional/lifecycle gates, not performance measurements.
