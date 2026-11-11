# Provider-info refresh validation

This normal-library component implements absent, stale, and forced Movie/Series provider-info refresh using the existing VOD admission and memory ledger. Movie preparation preserves the later task reread; Series preserves its earlier parent/account snapshot and subsequent relation reads. Metadata saves, episode transactions, flags, partial failures, and sequence effects follow the observed Source phases.

The reviewed union70 contains 734 Rust inputs: pinned base `6e15b9315871db97b548c262a99eeeccc24fdcbd` plus 36 selected runtime/test assets. Four Source-derived test datasets repair the earlier incomplete 32-path proposal. The failed 730-input compile remains retained; it is not an accepted closure. The accepted union passed strict workspace/all-target Clippy and 452 default/doc tests (631 external tests ignored).

Actual pinned Original TCP versus standalone native HTTP passed 205 supported responses out of 207 captured cases. The two fixed-clock APIRequestFactory references are separate exclusions from this live TCP comparison. Of the 205 responses, 128 bodies match raw bytes; 77 use only declared newly-created Episode UUIDv4 or causal Series timestamp substitutions, each checked against its own committed SQL. All other body spans, whitespace, ordering, status, selected headers, provider request wire, and SQL effects remain strict. This is not general JSON normalization. Five task-boundary/caller-loss schedules and four actual PostgreSQL/Redis physical-ownership tests also passed on the same ELF. Each disposable fixture restored the selected 18 models and sequences; independent project container/network/volume checks were empty.

These are component and standalone-router results. Canonical Main packaging, stale-modal browser behavior, and performance acceptance require subsequent gates. HTTP body completion is not a TCP socket-close acknowledgment. Broad uncaptured Requests CookieJar grammar and corrupted internal phase-invariant recovery are not accepted by these tests. No new performance gain is claimed.

## Reproduce from committed objects

Use a full repository clone with the pinned base and Original commit `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` available. The reconstructor reads only the pinned base archive and individually named Git blobs, then verifies all Rust and tool SHA256 values. Missing objects fail; it never copies current worktree or private artifact files. The reviewed runtime files must have been published before reconstructing their blobs.

```sh
python3 scripts/rust-vod-api-contracts/reconstruct_provider_refresh.py \
  --repo . --output /tmp/vod-refresh-union70
cargo test --offline --locked \
  --manifest-path /tmp/vod-refresh-union70/rust/Cargo.toml \
  --target-dir /tmp/vod-refresh-union70-target \
  -p dispatcharr-store --lib --no-run
```

Use the Store test ELF reported by Cargo as `--native-actor-binary`. Always use a fresh target for this exact source path; do not reuse a target compiled from another copied source tree. The recorded union70 ELF SHA256 is in the verified JSON, but newly compiled ELF bytes may differ with toolchain/path. Source hashes establish which code was compiled; preserve the actual compiler log and resulting ELF hash.

The fixture also needs Docker Compose, the repository's pinned Source dependency image, and a successful build manifest for a donor image containing the unchanged compiled React frontend. `--build-manifest` identifies and inspects that image; its Rust binary is never used in these component tests. The historical run used image44 as a frontend donor. A fresh immutable donor can be built with `scripts/rust-integration/build.py --git-revision 6e15b9315871db97b548c262a99eeeccc24fdcbd --tag dispatcharr-rust-candidate:vod-refresh-frontend --output <new-build-directory>`. Record its actual manifest and frontend hashes rather than relabeling it as historical image44.

Run the reconstructed tools from the repository root. Supply the actual donor manifest and compiled ELF paths:

```sh
GIT_DIR="$PWD/.git" GIT_WORK_TREE=/tmp/vod-refresh-union70 \
python3 /tmp/vod-refresh-union70/scripts/rust-vod-api-contracts/run_provider_refresh_source.py \
  --source-only --native-actor-binary <Store-test-ELF> \
  --build-manifest <frontend-build-manifest.json> \
  --observer provider_refresh_full_http_source.py \
  --case-filter movie_auth_cookie,series_null_credentials \
  --output <new-evidence-directory>
```

The explicit Git bindings prevent nested-working-directory `git archive` from silently producing a subtree. The driver checks the complete Original archive SHA256 `f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff`, creates a unique owned project, traps external provider contacts, uses only its local provider origin, and removes its owned resources. Outputs containing JWTs, raw provider credentials, or complete fixture state remain private; publish only selected sanitized counts and hashes.

The exact validated label groups are published in `provider-refresh-http-cases.json`:155 semantic records (including two explicit Factory exclusions) and52 transport records. Pass a comma-joined group as `--case-filter` to reproduce each bounded run; the retained union70 execution ran them separately. Omitting the filter requests the full currently declared Source corpus and may need a longer observer timeout. For task schedules use `--observer provider_refresh_schedule_source.py --schedule-case` with `movie_third_recent_skip`, `series_third_old_snapshot`, `series_third_old_account_agent_relation`, `movie_caller_lost`, or `series_caller_lost`. The Source reference uses a labeled direct-task wrapper on its owned threaded WSGI worker; it does not claim Source has the native two-upstream admission cap. For four real physical tests use `--observer provider_refresh_fair_physical.py`; it pauses only its disposable Redis and never an external/shared service.

The verified JSON retains negative-attempt classifications, the original incomplete-closure failure, Source/reference distinctions, and new public-tool replay bindings. Original Dispatcharr authors retain application and algorithm credit; the existing repository licenses and attribution remain authoritative.

Publication whitespace repair: one trailing space at scheduler-source line157 was
removed with identical Python AST. The pinned tool Git blob, manifest and
reconstructor digest were updated. All734 runtime inputs remain identical. The
original205 HTTP/five schedule/four physical execution hashes remain historical,
unchanged; corrected public tools separately passed a fresh queued-Movie schedule
and four actual physical controls after fresh Git-only reconstruction. No full
205-case rerun or new compilation is claimed for this one-byte tool repair.

Historical module headers containing “prototype” or “unwired” predate the normal
registration in this checkpoint. Current module declarations, immutable source
map and actual gates define this component’s scope. The public reconstructor
requires Python with `tarfile` data filtering (Python3.12+ is suitable).
