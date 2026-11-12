# Frozen Compose SDK qualification: 9175d2c5

Original Dispatcharr and dependency authors retain application, algorithm and licensing credit. This is core SDK verification, not application/Main execution, resource comparison or completion of other milestone gates.

The unchanged `scripts/verify-rust-compose.py` ran once from a complete immutable archive of commit `9175d2c56aff107174444e8518bc31700e756a6b`. A separate private detached HEAD, index, worktree and object destination bound its Git commands to that archive; the canonical objects were read-only alternates. The verifier therefore recorded 9175 rather than a live worktree or later HEAD. All 6,158 archived source files retained their bytes, all 4,939 verifier-selected hashes matched, and the 791 Rust entries equal the parent `a729127b...` map.

Actual results were Rust **578 passed / 705 ignored** across 183 summaries and Python **316 passed / one skipped** across 317 tests. The Python contract subset is 300 total / 299 passed / one skipped; the remainder is 15 `rust-perf` and two `rust-parity` tests. The unchanged standard SDK also produced 1,806 byte-exact TS-helper comparisons and its fanout/TS-helper microbenchmark artifacts. Those helper artifacts are separate from application/core-buffer resource acceptance; this packet makes no new performance claim.

The actual image is `sha256:e3c96bf8d92cf51d62a294a8de968d4b77825d6f51a62bfc0562e8a88e4d915c`. Its verification container ran with network `none`, Cargo output only at image-internal `/tmp/dispatcharr-target`, and actual die exit0 followed by destroy. Independently queried project container/network/volume inventories were empty. No host union target, canonical index/ref or runtime source was modified by this run.

Two review negatives remain distinct. The first auxiliary image checker incorrectly combined the actual 1,159 Docker COPY inputs with two host-only controls (`docker/compose.rust.yml` and `scripts/verify-rust-compose.py`), so its retained result is `passed:false`. It did retain every actual copied-file hash. The separate offline proof derives the full COPY set from the pinned Dockerfile, verifies all 1,159 actual hashes, verifies exactly those two host controls against the frozen Git/selected maps, and never relabels the original checker. A first read-only cleanup review also expected capitalized Docker absence text; the actual lowercase `error: no such object` diagnostic, exit1/empty array, exact destroy event and empty inventories were retained and correctly classified in a separate review. Neither issue caused an SDK rerun or any execution/input change.

`raw-manifest.json` binds the exact logs, actual commands/environments, maps, image/event/configuration records, cleanup and negative records. The five `inputs/` files are exact committed build/verification controls. `executed-tools/` preserves the actual private capture/monitor/review source for provenance; its historical artifact locations are observations, not public rerun prerequisites.

## Revalidate without execution

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/rust-integration/sdk9175/validate.py --repo .
PYTHONDONTWRITEBYTECODE=1 python3 scripts/rust-integration/sdk9175/test_packet.py
```

These commands read retained evidence. Optional `--repo` reconstructs the immutable Git archive in memory and compares all 6,158 file hashes. No Docker, compiler, Source/application process or network is invoked. Thirteen pure controls include semantic corruptions after resealing their fixture bytes, so they test more than the raw manifest. The initial pure-test setup omitted the historical executed-tools directory; its failed log is preserved separately from the corrected thirteen-pass controls.

## Reproduce the standard SDK run

Use an ordinary supported Docker/Compose environment and Git with this commit available. Choose a fresh output/project. The following example creates private Git metadata; all build inputs come from the committed archive, and no private capture/support path is required. The standard script keeps its default 1,800-second timeout and cleans only its owned Compose project.

```sh
repository=$(pwd)
work="$repository/artifacts/rust/sdk-rerun-9175"
mkdir -p "$work/source"
git archive --format=tar 9175d2c56aff107174444e8518bc31700e756a6b | tar -x -C "$work/source"
git init --bare --template= "$work/git-metadata"
printf '%s\n' "$repository/.git/objects" > "$work/git-metadata/objects/info/alternates"
chmod 444 "$work/git-metadata/objects/info/alternates"
printf '%s\n' 9175d2c56aff107174444e8518bc31700e756a6b > "$work/git-metadata/HEAD"
git --git-dir="$work/git-metadata" config core.bare false
git --git-dir="$work/git-metadata" config gc.auto 0
export GIT_DIR="$work/git-metadata"
export GIT_WORK_TREE="$work/source"
export GIT_INDEX_FILE="$work/git-metadata/index"
export GIT_OBJECT_DIRECTORY="$work/git-metadata/objects"
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_NOSYSTEM=1
export GIT_OPTIONAL_LOCKS=0
git read-tree 9175d2c56aff107174444e8518bc31700e756a6b
git rev-parse HEAD
git status --short
python3 "$work/source/scripts/verify-rust-compose.py" --project dispatcharr-sdk-9175-rerun1 --evidence-dir "$work/evidence"
```

Start from a shell without inherited conflicting Git variables. Inspect actual process exits, source hashes, image identity and independent owned cleanup for a new run. A new image/binary identity or later source tree does not inherit this captured execution automatically.
