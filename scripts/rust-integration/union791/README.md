# Combined 791-input Rust qualification

Original Dispatcharr authors and dependency contributors retain all application, algorithm and licensing credit. The committed Rust dictionary at `f0bfc2f200dcfd1f64ae32a9855ea250731ce67f` is byte-identical to the frozen integration candidate: 791 files, map SHA256 `a729127b8f57f261433152d958b60695fb9e25ce824b65678b614378ac6bc8c5`. Five separate timeshift golden inputs are also bound.

Actual qualification on this combined source:

| Gate | Result |
| --- | --- |
| Formatting, non-test workspace build, strict all-target Clippy | All exit 0 |
| Default workspace tests | 578 passed, zero failed, 705 explicitly ignored; 183 result summaries |
| URL import comparison | 17 cases / 393 checks; 1,070 temporal fields retained |
| Notification SQL/router comparison | Eight Source-derived requests plus eight Native controls |
| Worker-header Redis/media regression | 95 checks across ten unchanged batches |

Root independently checked the raw comparisons, exact executable/source/tool bindings, database audits appropriate to each fixture, and owned cleanup. The header fixture has no SQL/default-database acceptance. The import fixture's Original producers/fallback workers remain distinct from Native execution; no fourth Original URL-reference capture occurred. The notification Source observations use forced authentication, distinct from Native API-key HTTP. Fixture-dependent tests skipped by the default run are not implicitly passed.

`host-checks.json`, the compressed raw compiler/test output, outside-peer receipts and the URL17 raw observations preserve actual results. `evidence-inputs.json` hashes those payloads; `gzip -dc host/defaults.stdout.gz` reproduces the original stdout and its separately recorded digest. The historical seven common URL-controller files are byte-identical to their existing public component paths in `url17/common-controller-bindings.json`; only explicit source-map/compiler/ELF receipts changed for this combined run. Historical executable paths are provenance, not binaries distributed by this packet.

The [notification packet](../../rust-contracts/notifications-native/README.md) contains the combined eight-plus-eight raw SQL/response/clock/sequence proof. The [worker packet](../../rust-vod-contracts/worker-identity/union791/README.md) contains the ten-batch executor, raw logs, corruption controls, Git-only source reconstruction and fresh-build/replay helper. Those packets distinguish retained results from a new compilation or execution. Fresh ELF paths/hashes must receive new explicit receipts; old receipts cannot qualify a substitute binary.

From a checkout containing these public tools, reconstruct a fresh exact source and run the ordinary workspace gates:

```sh
python3 scripts/rust-vod-contracts/worker-identity/union791/reconstruct.py --repo . --output /tmp/NEW-union791-source
CARGO_TARGET_DIR=/tmp/NEW-union791-target cargo fmt --manifest-path /tmp/NEW-union791-source/rust/Cargo.toml --all -- --check
CARGO_TARGET_DIR=/tmp/NEW-union791-target cargo clippy --manifest-path /tmp/NEW-union791-source/rust/Cargo.toml --locked --workspace --all-targets -- -D warnings
CARGO_TARGET_DIR=/tmp/NEW-union791-target cargo test --manifest-path /tmp/NEW-union791-source/rust/Cargo.toml --locked --workspace --all-targets
python3 scripts/rust-contracts/notifications-native/validate.py
python3 scripts/rust-vod-contracts/worker-identity/union791/validate.py
```

Use Rust 1.90, unique output/target paths and the documented fixture prerequisites. The validators read retained evidence; they do not execute the application. Dedicated fresh fixtures have their own finite ownership, input-binding and cleanup requirements.

Main/default worker activation, full endpoint/role coverage, the SDK run, a new packaged application image, broad Main replay and performance are separate gates. Image52 remains the accepted packaged/measured application until its successor passes those gates. This component evidence adds no whole-milestone, rewrite-completion or new performance claim.
