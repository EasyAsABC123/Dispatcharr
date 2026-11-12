# Exact retained-result component reproduction

Original Dispatcharr and dependency authors retain all application/algorithm credit.

`reconstruct.py` passed an actual Git-only reconstruction of the historical763-input qualification: committed6054 plus its exact four selected blobs. It also reconstructs pinned support scripts, including compile-time golden fixtures. `verified-reconstruction-1.json` records that check. All selected blobs must be reachable in Git; missing objects and mutable/private fallback are errors.

From this directory, use fresh absolute output paths, Rust/Cargo1.90.0, cached locked dependencies and the pinned Docker fixture image:

```sh
python3 reconstruct.py --repo /absolute/clone --output /absolute/fresh/source
mkdir /absolute/fresh/cohort
cp run_regressions.py /absolute/fresh/cohort/
CARGO_TARGET_DIR=/absolute/fresh/target cargo test --manifest-path /absolute/fresh/source/rust/Cargo.toml -p dispatcharr-coordination -p dispatcharr-vod-http --tests --no-run --offline --locked --message-format=json > /absolute/fresh/cohort/test-binaries.jsonl
python3 /absolute/fresh/cohort/run_regressions.py
```

The runner requires newly built ELF records and generates/removes its owned Redis/media fixture. Preserve logs, response prefixes, ELF hashes and failure reports, and verify exact-owned cleanup. Git reconstruction alone is not a fresh compilation/execution claim. The historical763 cohort has its own77-test evidence; the later combined766 source was separately rebuilt and passed77 on new binaries. `source-inputs-union766.json` and `verified-union766-1.json` distinguish those inputs and observations. Neither establishes packaged Main or performance acceptance.
