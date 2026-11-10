# Bounded EPG file import (application-unwired)

All application and algorithm credit belongs to the original Dispatcharr authors and contributors. This crate composes their XMLTV preparation/projection and staged PostgreSQL swap, using the separately verified Rust components. It is not an accepted full refresh job or an application route.

The service admits at most2 physical jobs by default (hard maximum4), without an unbounded waiter queue. Each job has one blocking native parser, a2-batch channel and one owned SQL staging session. A batch contains at most1000 rows and4MiB of conservatively charged retained row memory; hard maxima are2500 rows/8MiB and4 queued batches. Input files are regular files below4GiB, with up to5million accepted mapped programmes. Mapping snapshots retain the Store's100,000-entry/16MiB bounds. XML record extraction remains bounded by the native parser component. Limits reject the job; they never silently truncate a successful guide. Larger valid Source files/records remain unsupported until a further scoped gate.

The same open descriptor passes the Source8192-byte literal `</tv>` suffix heuristic, resets to byte0 for exact HTML4 DTD prefix handling, and feeds the parser. Descriptor identity/length/change timestamps and the literal footer are rechecked before a successful parser result. Recover-mode XML behavior follows the original parser, rather than requiring strict well-formedness. Programme projection errors are skipped independently of unmapped-record counts. A source with no mapped EPG entries returns before file access, matching the original early return.

A caller supplies a retained job/lock guard. Dropping its awaiter requests cancellation, while the owned task joins the physical parser and pending SQL reply. Closing the bounded queue wakes a parser blocked on a full channel. Temporary staging data is not swapped after parsing, capacity, I/O or cancellation failure. Shutdown signals all admitted jobs and reports false at its deadline if physical work remains; it never abandons a blocking parser or releases its guard early.

`GuideStaging::swap_cancellable` checks the cancel flag before the private transaction starts and immediately before commit. A cancellation observed at that check rolls back deletion/insertion. This is a commit-check boundary, not atomic fencing across Redis and PostgreSQL or a guarantee that cancellation arriving after the check prevents commit. The original `swap` entrypoint delegates with an unset flag.

Five component tests cover row/skip/error routing, incomplete files, retained string capacity, a parser blocked on a full queue, and mutation after staging begins. The actual 15-case original file-to-guide SQL differential and six physical failure/provider scenarios now pass; the service remains application-unwired pending complete refresh-job integration. See scripts/rust-epg-parity/verified-file-guide-cached-clock-46.json. The separate Store hook already passes15 original staging/swap cases and a blocked SQL rollback/physical-guard regression.

Source cache fetching/decompression, first/late mapping orchestration, persisted source status and timestamps, progress/error notifications, audit/cache invalidation, scheduling/retry/completion effects and resource acceptance remain separate migration gates. This crate accepts server-controlled prepared file paths and explicit cutoff time/history policy; it exposes no user-supplied filesystem path API.

Post-quiet integration scenarios additionally gate a real private PostgreSQL staging INSERT with a fixture-only advisory trigger. They mutate the already validated file, exceed the parser count after staging starts, and cancel an aborted waiter while SQL is physically blocked. Each must leave live rows unchanged; cancellation must reject extra jobs and report incomplete shutdown until the actual SQL reply/parser drain releases the original guard. These scenarios passed in the disposable-schema import42 proof.

The optional `SwapPolicyProvider` hook is drafted for application integration. `import_file_with_swap_policy` resolves it only after parser completion and all staging replies, immediately before swap; deterministic `import_file` keeps the explicit request values. Empty accepted catalogues skip the provider, as Source retention computes no cutoffs. The provider receives a cancel flag and retained job context; clone that context into any supervised I/O that can outlive its waiter. The importer keeps awaiting owned provider work during cancellation, then rechecks cancellation before swap. Source samples its cutoff clock before loading history policy at this boundary; a cache-backed implementation must preserve that order. Actual cache/clock mutation and held-provider cancellation proofs remain queued after the resource quiet window.

Post-quiet reproduction commands (use the coordinated disposable Docker/toolchain environment):

```sh
cargo test --manifest-path rust/Cargo.toml -p dispatcharr-ingest-epg --locked --offline
cargo clippy --manifest-path rust/Cargo.toml -p dispatcharr-ingest-epg --all-targets --locked --offline -- -D warnings
python scripts/rust-epg-parity/run_import.py --output artifacts/rust/m6-file-guide-import-NEW
python scripts/rust-epg-parity/run_swap.py --output artifacts/rust/m6-guide-cancellable-followup-NEW
```

The prepared original oracle now has14cases, including a clock/history policy change after its first physical staging flush. Native SQL gates additionally prove that the late provider is not called during parsing or on incomplete/empty catalogues, is evaluated after staging, and observes the changed cutoff. A held async provider and a blocked SQL insert test caller-drop cancellation without manually setting the cancel flag; neither may release its guard or swap live rows until physical work drains. These additions passed after the resource quiet window in import42. The late policy fixture is a controlled clock/policy timing oracle, not proof of the future cache-backed provider's physical Redis lifetime.

The separate retained `CachedSwapPolicy` adapter passes18 original cached-policy cases and physical Redis/SQL guard tests. Original Django decoded12 native-filled cache values. It uses the Source `epg_settings` group and version keys, interoperable restricted cache encoding, Python-compatible version equality, exact group defaults and a bounded owned SQL fallback. It distinguishes connectivity failures from server/authentication errors. Its own physical admission travels with any delayed Redis/SQL guard. See scripts/rust-epg-parity/verified-retained-policy-cache-45.json. It remains unwired from application jobs.

The real cached clock-change proof now passes as part of import46: Source and native both change the shared `epg_settings` cache and cutoff clock after the first physical staging operation while leaving SQL settings stale. Resulting live rows match. The cache adapter independently passes18 Source cases, including Python-compatible version equality, wrong Redis types, bad authentication, backend fallback/bypass and malformed pickle. A timed-out Redis reply and an aborted blocked SQL read retain both caller context and physical admission until actual completion; a recovered connection reads the existing shared cache again.

The next separate Docker gate uses `rust/Dockerfile.ingest-epg-tests`. It builds only the two native component test binaries from the next canonical immutable archive. They run as UID65532 with a read-only root filesystem,512MiB,2CPUs and128PIDs, using the actual original migrated PostgreSQL/Redis fixture. No Python backend or application source is mounted into the native test container. The original Python process remains solely the independent oracle and infrastructure initializer.

```sh
python scripts/rust-epg-parity/build_import_tests.py \
  --build-manifest artifacts/rust/CANONICAL/build.json \
  --output artifacts/rust/epg-component-image-NEW
python scripts/rust-epg-parity/run_import.py \
  --output artifacts/rust/epg-component-import-NEW \
  --native-test-manifest artifacts/rust/epg-component-image-NEW/build.json
python scripts/rust-epg-parity/run_policy_cache.py \
  --output artifacts/rust/epg-component-policy-NEW \
  --native-test-manifest artifacts/rust/epg-component-image-NEW/build.json
```

The Docker component gate now passes using an explicitly derived archive that preserves all367 accepted import46 Rust files byte-for-byte and adds only test packaging plus LICENSE/ATTRIBUTION. Import51 passes15 original cases and six physical scenarios; policy52 passes18 original cases, physical Redis/SQL guards and original Django cache decoding. See scripts/rust-epg-parity/verified-import-policy-component-docker-51.json. This is an independent component image, not a canonical application image, refresh route or guide-import resource result.
