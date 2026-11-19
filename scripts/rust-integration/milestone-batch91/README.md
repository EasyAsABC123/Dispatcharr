# Candidate91 verification

The exact candidate package passed the frozen paired ordinary DVR playback
and Channel Batch EPG fixtures. [Acceptance JSON](acceptance.json) binds the
package, actual run results and cleanup.

The following replays the sanitized projection from retained local receipts.

```sh
python3 -B scripts/rust-integration/milestone-batch91/verify_acceptance.py \
  --candidate90-focused "$earlier_focus" --candidate91-focused "$exact_focus" \
  --manifest "$manifest" --m4-dir "$ordinary_dvr" --m5-dir "$batch_epg"
```

To run the same wire and SQL predicates from a checkout, use the exact package
manifest and its local tag with the optional suites. Use distinct empty
evidence directories for the two invocations.

```sh
python3 -B scripts/rust-integration/run.py --contract-suites recording_read \
  --candidate-image "$tag" --candidate-build-manifest "$manifest" \
  --output "$ordinary_dvr_output"

python3 -B scripts/rust-integration/run.py --contract-suites channel_batch_epg \
  --candidate-image "$tag" --candidate-build-manifest "$manifest" \
  --output "$batch_epg_output"
```

The published receipts came from the frozen runners. They retain exact
Original revision, package and verification-input hashes. The optional suites
use the same business predicates inside the checkout's integration harness.

The checkout-based batch guide rerun passed all 52 checks. An initial sparse
checkout omitted the existing origin helper and stopped before the first
check. Both projects were removed. [Portable Compose receipt](portable-compose.json)
