# Finite JSON formatter compatibility

Original Dispatcharr and dependency authors retain full code, algorithm and license credit. This corpus qualifies finite binary64 representation and the extracted Original app-data JSON sizing helper. It does not qualify entire endpoints, ORM behavior, nonfinite values or performance.

The old generic Rust formatter disagreed with Python in 259 of the original 9,338 cases. The replacement uses the already pinned serde_json shortest round-to-even digits and applies Python's notation thresholds, exponent spelling and signed-zero behavior. No dependency or JSON acceptance bound changed. The new corpus contains every old bit pattern plus 30 signed threshold and extreme-value neighbors, in a new sorted order. All 9,368 cases match across five typed fields (46,840 comparisons), on both the separately built private767 peer and the combined771 peer. Captured logs and exact hashes accompany the results; the combined tree also contains independently scoped scanner recovery changes.

The actual same-owned-file consumer corpus has 512 vectors and zero generic-formatter differences after this fix, with 5,135 SQL/cache/Channels checks and four physical controls. Historical scanner records reporting nine generic differences remain unchanged. The scanner's separately bounded epoch formatter is unchanged. This is a new generic-formatter profile, not a relabeling of old results.

Run the evidence verifier and corruption controls without Docker or Cargo:

```sh
CORPUS=scripts/rust-job-contracts/json-float
python3 "$CORPUS/test_verify.py"
python3 "$CORPUS/verify.py" --reference "$CORPUS/source-reference.json" --peer-log "$CORPUS/peer-union771.log"
```

The 15 controls reject wrong types (including booleans used as integer lengths), malformed or nonfinite bits, omitted/duplicate/reordered cases, incorrect UTF8 sizes, wrong same-length digits, duplicate peer payloads and missing successful peer-test receipts. They do not independently rerun Original or authenticate a forged evidence file; use the published hashes and Git provenance.

To rerun the exact finite peer from a checkout containing the fix, use a fresh Cargo target and build only that source tree:

```sh
export CARGO_TARGET_DIR="$PWD/artifacts/rust/target-json-float-reproduction"
cd rust
cargo test --offline --locked -p dispatcharr-store --lib json_float_peer::actual_finite_float_peer -- --exact --ignored --nocapture < ../scripts/rust-job-contracts/json-float/source-reference.json > ../artifacts/rust/finite-peer-reproduction.log
cd ..
python3 "$CORPUS/verify.py" --reference "$CORPUS/source-reference.json" --peer-log artifacts/rust/finite-peer-reproduction.log
```

To recapture Original, extract `apps/accounts/app_data.py` from pinned revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` to a regular mode0444 file. Its SHA256 must equal the module hash in `current-proof9368.json`. Mount that file at `/original-app-data.py` and the exact mode0444 `source_capture.py` at `/capture.py` in the pinned Original image recorded in that proof. Run `/dispatcharrpy/bin/python /capture.py` with `input-plan.json` on stdin, network disabled, readonly root and UID65532. The script executes only the actual extracted `AppDataError` and `json_byte_size` AST definitions with the required JSON/error-code globals, plus Python repr and UTF8 serialization; it does not import or execute the full Django module. Save stdout as a new reference, check bit identity/order, and compare a freshly built peer. The historical initial mode0600 refusal and incorrect generic ASCII oracle are retained privately and explicitly described in the proof; neither is accepted as the corrected reference.

Runtime payloads retain the exact historical three-path private767 source for inspection. They are not instructions to overwrite future canonical source. Build the requested checkout or a fully verified immutable reconstruction; never copy a Cargo target between source roots. Package-level regression and new numerical performance evidence remain separate gates.
