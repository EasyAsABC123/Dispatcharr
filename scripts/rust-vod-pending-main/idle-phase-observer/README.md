# Reproduce the scoped idle-observer component

This additive public recipe supersedes the portability assumption in historical
REPLAY.md without editing that executed document or its logs. It reconstructs
exactly the qualified prototype's767 Rust files, not the later772 union or Main
image. Original Dispatcharr and dependency authors retain their credit/licensing.

Inputs are immutable Git208598ee1872732cbe8a0d846efa5a40b59ae1d2, the hash-bound
three-path selected-source.patch, and the sealed reconstruction-manifest.json.
The helper derives the expected766-file ancestor map from final767 plus each
selected ancestor hash, checks every base file, applies only exact context/count
verified hunks on the3allowed paths, then checks every final file. It also restores
and verifies all5 external timeshift Golden JSON inputs from the SAME Git revision.
No mutable working-tree or private artifact fallback, Git fetch, index/ref update,
Cargo, Docker or application execution occurs during reconstruction. The full base
commit must already be available locally; a missing object is a failure.

From a fresh clone containing these published tools and that immutable base:

```sh
VOD_OBSERVER_REPO=$(git rev-parse --show-toplevel)
VOD_OBSERVER_REPLAY="$VOD_OBSERVER_REPO/artifacts/rust/idle-observer-public-replay-new"
test ! -e "$VOD_OBSERVER_REPLAY"
python3 "$VOD_OBSERVER_REPO/scripts/rust-vod-pending-main/idle-phase-observer/reconstruct.py" \
  --repo "$VOD_OBSERVER_REPO" --output "$VOD_OBSERVER_REPLAY/source"
cp "$VOD_OBSERVER_REPO/scripts/rust-vod-pending-main/idle-phase-observer/run_regressions.py" \
  "$VOD_OBSERVER_REPLAY/run_regressions.py"
cd "$VOD_OBSERVER_REPLAY"
CARGO_TARGET_DIR="$VOD_OBSERVER_REPLAY/target" cargo test --offline \
  --manifest-path source/rust/Cargo.toml -p dispatcharr-vod-http \
  -p dispatcharr-coordination --tests --no-run --message-format=json \
  > test-binaries.jsonl 2> compile.log
CARGO_TARGET_DIR="$VOD_OBSERVER_REPLAY/target" cargo test --offline \
  --manifest-path source/rust/Cargo.toml -p dispatcharr-vod-http --lib \
  observation::tests -- --nocapture
CARGO_TARGET_DIR="$VOD_OBSERVER_REPLAY/target" cargo clippy --offline \
  --manifest-path source/rust/Cargo.toml -p dispatcharr-vod-http \
  -p dispatcharr-coordination --all-targets -- -D warnings
python3 run_regressions.py
```

Use one fresh target per exact source path. The regression runner reads the newly
built Cargo JSON IN THAT artifact root; it does not require historical private
ELFs. It copies those exact peers outside target, generates pinned local MP4, runs
one bounded owned Redis/media container (network-none, local internal listeners),
and removes its exact fixture. Docker/local socket permission and the pinned
Original image must be available. No real provider is contacted. Compile-time
coupled fixtures are included by reconstruction; Cargo's cached dependencies remain
an external prerequisite for --offline. Do not run this during a coordinated quiet
measurement window.

Historical qualification:10 pure observer controls +strict coordination/vod
all-target checks and80 actual native component regressions (old77 +4s success,
busy and client-loss3). Ordinary2s behavior, actual error/lost reply/no replay and
physical ownership were retained. The first60s reserve limit is scalar-qualified;
full redis-py timeout/retry equivalence is not claimed. Component fixtures use a
resolver stub, so they do not establish SQL/Bearer/Main or complete M4 parity.
Root772's separate execution proof and image50's valid4s parity negative remain
separate historical authorities.

The additive reconstruction controls and one actual fresh767+5-file reconstruction
pass with no new Cargo/application run. These validate public input closure only;
they do not transfer old execution to newly compiled or differently bound binaries.
