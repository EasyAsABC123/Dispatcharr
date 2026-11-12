# Exact-source component replay

Use this frozen source/rust (Git208598ee plus selection.json three paths), not
working-tree VOD files. Build fresh targets per source path. Canonical assembly,
new image and packaged Main validation belong to Root and have not occurred.

The executed commands were:

```sh
CARGO_TARGET_DIR="$PWD/target" cargo test --offline \
  --manifest-path source/rust/Cargo.toml -p dispatcharr-vod-http \
  -p dispatcharr-coordination --tests --no-run --message-format=json \
  > test-binaries.jsonl 2> compile.log
CARGO_TARGET_DIR="$PWD/target" cargo test --offline \
  --manifest-path source/rust/Cargo.toml -p dispatcharr-vod-http --lib \
  observation::tests -- --nocapture
CARGO_TARGET_DIR="$PWD/target" cargo clippy --offline \
  --manifest-path source/rust/Cargo.toml -p dispatcharr-vod-http \
  -p dispatcharr-coordination --all-targets -- -D warnings
python3 run_regressions.py
```

Run only in a fresh artifact root: the runner refuses existing native-peers/media
output directories. It copies exact compiled ELF paths from Cargo JSON outside
target, generates deterministic local MP4 with pinned Original image ffmpeg, then
runs real Redis and fixture HTTP listeners inside one owned network-none container.
No actual provider traffic, SQL resolver, Bearer authorization or Main is used.
The three new4s HTTP cases require actual200/429 result, early200/8192 follower,
private other reservation preservation, caller-loss settlement and no replay.
Old77 include actual Redis errors/lost reply/ordinary2s/lease/stop/drain/fence cases.
The Source first60s read/failedsocket retry boundary is not physically qualified.

Preserve prior prototype1 and fixed4s image50 negative evidence. Successful component
validation cannot rewrite either historical failure or authorize packaged replay.
