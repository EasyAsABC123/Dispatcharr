# Reconcile Candidate92 System Logs checks

The [acceptance receipt](acceptance.json) binds the exact Original and Native
images, role and wire comparisons, shared read-only mount, Store verification
and physical cleanup. For a retained private run and its exact build inputs:

```sh
python3 -B scripts/rust-integration/milestone-batch92/verify.py \
  --run-dir "$logs_run" --manifest "$candidate92_build" \
  --reference-manifest "$candidate91_build" --focus-dir "$candidate92_store"
```
