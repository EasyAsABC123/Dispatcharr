# Successful VOD worker identity component

This component adds the Source `X-Worker-ID` rule to successful VOD stream responses. Identity uses this process's actual kernel hostname and PID, cached across engine instances, threads and header clones. It ignores overrideable `HOSTNAME` and provider-supplied worker headers. Hostname syscall failure selects the Source four-digit `worker-1000` through `worker-9999` fallback once. Header conversion failures do not silently remint identity.

Original Dispatcharr and dependency authors retain application, algorithm and license credit. The pinned Source manager sets the header on its successful 200/206 response builder. Actual image52 Source 200 has the header while native 200 omitted it; busy 429 lacked it on both sides. Public VOD URLs route to GET-only `stream_vod`, so HEAD remains 405. The separate `head_vod` helper is not routed there and is not newly exposed. Pinned Source excerpts and prior raw-map references are included.

The five-path overlay is qualified against canonical `98e447186af099dd17496539aec46312f6cf4f62`: 788 base Rust inputs become 789. The new direct edge is pinned nix 0.31.3 with only its hostname feature; no registry package or package version was added. No unsafe FFI or hostname crate is introduced. The Source-compatible Latin-1/encoded-word conversion is bounded; nine pinned Django header primitive vectors cover ordinary and Unicode values, CR/LF errors and the separate NUL distinction. This primitive initialized no application or database and had no network access. A real hostname C string terminates at NUL; the native header guard rejects injected controls. Full invalid-hostname network/error-message equality is not claimed.

Qualification on the exact private snapshot:

- Non-test library build and strict affected-package all-target Clippy passed.
- 80 package default test executions passed; 238 fixture-dependent tests were ignored across 14 test/doc result suites. Counts are executions, not unique semantic contracts.
- 95 real Redis/media controls passed in 10 batches: broad HTTP 15; retained result 6; ordinary two-second control 1; lease physical 13; pending follower 4; idle entry 5; generation scope 20; physical scope 9; pending HTTP 9; recovery HTTP 13.
- The two exact owned network-none media/Redis containers are independently absent. No Docker network or volume was created. All 15 unit/integration ELF hashes are retained; the actual fixture executed eight selected binaries.

This is native component evidence with fixture resolvers, not actual Main SQL/auth, a new app-image replay or Source application execution. Existing Connection/header differences, public counter/timer decisions and recovery defaults remain unchanged. Main, new canonical image, full M4 and performance acceptance remain pending. Image52 remains the accepted package; no image52 gains transfer to this changed component.

## Verify and reconstruct

From a full Git clone with Python 3.12 or newer:

```
python3 scripts/rust-vod-contracts/worker-identity/verify.py
python3 scripts/rust-vod-contracts/worker-identity/test_reconstruct.py
python3 scripts/rust-vod-contracts/worker-identity/test_verify.py
python3 scripts/rust-vod-contracts/worker-identity/reconstruct.py --repo /path/to/clone --output /tmp/worker-identity-NEW
```

Reconstruction reads immutable Git revision 98e from the repository root, verifies its full `rust`/`scripts` archive and all 788 base hashes, then applies exactly five bundled blobs and verifies all 789 qualified hashes. It refuses missing/corrupt/symlink payloads, unsafe paths and existing outputs. It does not read current mutable source, use private artifacts or mutate Git. The public helper was actually exercised once and matched the qualified snapshot; no new compilation or application run is inferred from that equality.

To repeat the native fixture with Docker, the pinned Original image, cached Cargo packages and permitted loopback sockets, compile that reconstructed tree into its own fresh target. Preserve JSON compiler output for both package selections:

```
CARGO_TARGET_DIR=/tmp/worker-identity-NEW/target cargo test --manifest-path /tmp/worker-identity-NEW/rust/Cargo.toml -p dispatcharr-vod-http --tests --no-run --offline --locked --message-format=json > /tmp/worker-identity-NEW/test-binaries.jsonl
CARGO_TARGET_DIR=/tmp/worker-identity-NEW/target cargo test --manifest-path /tmp/worker-identity-NEW/rust/Cargo.toml -p dispatcharr-coordination --test retained_reserve --test lease_physical --no-run --offline --locked --message-format=json >> /tmp/worker-identity-NEW/test-binaries.jsonl
cp scripts/rust-vod-contracts/worker-identity/run_native.py /tmp/worker-identity-NEW/run_native.py
python3 /tmp/worker-identity-NEW/run_native.py
```

`run_native.py` is byte-identical to the qualified private runner. It derives its fresh fixture root from its own location, copies the selected compiled executables, generates the deterministic local media and runs the existing ignored native controls. Its resolver is deliberately a fixture; do not relabel this as packaged Main/SQL/auth or a new Source experiment. Actual media, binaries and private build logs are not published; hashes and scoped results are. `verified.json`, `cleanup.json`, input maps and reconstruction checks preserve the exact authority boundary.
