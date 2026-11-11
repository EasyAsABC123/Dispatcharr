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

The [image47 replay](verified-shared-ts-image47-1.json) repeats all six
Source/native cases against the immutable measured image. All 34 checks execute;
33 pass. Healthy Original protocol and native functional/recovery checks pass,
including 12 bounded audio/video decodes. Original owner death again leaves one
provider slot and an input index after 90 seconds, so the combined report remains
failed. Native cleanup completes in 26.48 seconds with no automatic takeover.
The retained raw report, exact helper hashes and independent zero-owned-resource
check are bound in the public projection. This does not imply identical crash
state, a new resource benchmark or complete M2 acceptance.

The [image47 retained-media replay](verified-retention-image47-1.json) separately
passes ten live channels sampled through75 seconds and100 late historical
readers at32-way concurrency. Each late reader matches an exact Redis history
prefix35–55 seconds old, consumes1,500,000 bytes, and opens no additional
upstream. Twenty captured streams decode audio/video. Local payload remains
below4MiB per channel while authoritative shared history retains its separate
16MiB/channel and60-second limits. Final observed payload is40,938,880 local
bytes and150,962,120 shared Redis bytes; these are payload diagnostics, not
cgroup working memory or application resource gains.

Reproduce with `retention_probe.py` and the same image/build-manifest arguments
as the two-worker runner, adding `--expected-local-limit 4194304` and
`--late-history-viewers 100`. The default16MiB local expectation belongs to an
older runtime; its failed attempt is preserved. Helper hashes, source/image
bindings, captures, decoder frame counts and independently empty owned resources
are recorded. Use a fresh output directory. This remains a native functional
accounting check, not an Original/native performance comparison.
