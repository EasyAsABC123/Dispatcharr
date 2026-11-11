# Bounded logo scanner task and inert application

Original Dispatcharr and dependency authors retain application, algorithm and license credit. This qualifies the logo-only domain; it does not complete the general file-scanner family.

The exact source is immutable base `1c5196a01808ed51edca60a20d1b3d0b736e3975` (748 Rust inputs) plus 18 hash-checked Git blobs, yielding the full 759-input map. No Cargo dependency, Main registration, scheduler installation or default consumer is added. Normal library modules are tested through the same explicit constructors used by application assembly; only peers and controls are test-only. See [READY-API.md](READY-API.md).

Actual Source producer/native consumer/Original AsyncResult: 85 checks and five physical controls passed. Legacy notification intake preserves a genuine scanner frame before ACK, expiry or effects; the explicit logo application subsequently consumes that same frame. The original default-pair scanner-negative remains unchanged and passed. Ordinary timestamp/effect replay passed 5,135 checks and four physical controls, including 512 same-inode stat/formatter vectors. The existing generic float formatter remains unchanged; its nine observed decimal tie differences are retained. Whole-workspace strict/all-target Clippy, non-test check and 474 default tests passed (658 ignored).

A fresh Git-only reconstruction and fresh two-peer build passed the same actual 85+5 task and 5,135+4 ordinary replays. Eleven reconstruction and five builder corruption controls passed. The new build has its own recorded ELF hashes; no historical ELF or performance transfer is claimed. `verified-scanner-task-4.json` binds reports, maps, compilers and retained failures.

## Reproduction

Run from a clone containing the selected overlay blobs and the pinned Original revision. Missing Git objects are an error; no mutable/private source fallback exists. Use Rust/Cargo 1.90.0, cached locked dependencies, Docker Compose and the pinned Original image required by the published fixture support. The output and target directories must be new and outside frozen source.

```sh
TASK=scripts/rust-job-contracts/scanner-logo/task
python3 "$TASK/test_reconstruct.py" --repo "$PWD"
python3 "$TASK/reconstruct.py" --repo "$PWD" --output artifacts/rust/scanner-repro-source
python3 "$TASK/test_build_peer.py" --source artifacts/rust/scanner-repro-source
python3 "$TASK/build_peer.py" --source artifacts/rust/scanner-repro-source --target artifacts/rust/target-scanner-repro --output artifacts/rust/scanner-repro-build
python3 "$TASK/run_pair.py" --repo "$PWD" --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/scanner-repro-build/dispatcharr_job_transport --task-reference "$TASK/source-task-reference-1.json" --resources --output artifacts/rust/scanner-repro-task
python3 "$TASK/ordinary/run_pair.py" --repo "$PWD" --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/scanner-repro-build/dispatcharr_job_transport --float-binary artifacts/rust/scanner-repro-build/dispatcharr_store --reference scripts/rust-job-contracts/scanner-logo/source-logo-scanner-2.json --mtime-reference scripts/rust-job-contracts/scanner-logo/mtime-ties/source-mtime-ties-1.json --resources --output artifacts/rust/scanner-repro-ordinary
```

Verify the explicit support file map before fixture execution:

```sh
python3 - <<'PYVERIFY'
import hashlib,json
from pathlib import Path
root=Path('scripts/rust-core-perf/support')
expected=json.loads(Path('scripts/rust-job-contracts/scanner-logo/task/support-inputs.json').read_text())
assert all(hashlib.sha256((root/name).read_bytes()).hexdigest()==value for name,value in expected.items())
PYVERIFY
```

Fixtures use owned copied `rust_probe_backup_jobs_*` databases, broker/result Redis DB8/9, owned cache/files and an internal network. They audit default database preservation and remove the owned project. Source-only references remain separate: original102, ordinary-mtime43, decimal-tie70 and task/ACK24. Paired task replay uses a Source producer and result consumer, not an Original worker; actual Original-worker behavior is established by those separate captures. Do not confuse periodic header acceptance with scheduler attachment.

## Qualified boundary

Owned real UTF8 directories only; finite epoch mtimes in `[100000000,10000000000)`, observed bounded nonnegative decimal cached markers, bounded paths/files, empty M3U/EPG directories and no eligible missing/SQLNULL programme index sources. Only the observed literal invalid marker has a typed scanner ValueError. Unsupported input is refused before claim where preflight permits; stable external configuration/files/SQL are required. First-scan state is process-local. External concurrent mutation/multiple independent scanners, helper/Index descendants, M3U/EPG import, symlinks/special files, broader Python float grammar, ETA/expiry/parented task forms and other failure shapes remain unsupported.

SQL/cache/event commands and filesystem work retain physical owners until completion. Backend denial/lost reply stays operational; native PUBLISH denial may leave metadata committed and does not establish Source pipeline-vs-Lua ACL parity. Native traceback is an honest opaque diagnostic, with no fabricated Python frames or raw-traceback equivalence. Old SUCCESS bytes and aged TTL are preserved after normal and failed reexecution. One-second hard-loss non-replay is bounded observation, not visibility-timeout proof.

No Main/default intake/HTTP202, packaged UID65532, full scanner-family or resource-performance claim is made. Before any opt-in image attachment, compose against the new canonical source and replay its exact source/ELF, including managed writable paths and retained shutdown.

## Combined backend integration

The committed backend also preserves the noninteractive user-creation CLI and pending-follower work. `source-inputs-union762.json` records its complete 762-input Rust tree; `verified-union762-1.json` binds fresh strict Clippy, 476 default tests (667 ignored), task 85+5 and ordinary 5,135+4 checks, legacy/notification/expiry replays, and 54 actual Redis/HTTP fixture regressions. Each copied-database replay preserved the default database and removed its owned project. These checks used newly built union binaries.

The 759-input reconstruction above remains a historical, independently reproducible qualification. `historical/store-lib-759.rs` retains its exact Store library blob in reachable Git history; the canonical Store library additionally exports the CLI module. Historical source comments describing drafts do not change the qualification boundary documented here.

Image48 remains the accepted packaged checkpoint. A separate image49 Main test returned 503 in delayed-provider success and busy cases; its timeout and fixture authority are under investigation. Component checks do not supersede that failure. Scanner Main/default worker activation, helper descendants, nonempty M3U/EPG directories, broader traversal, and resource-performance acceptance remain open.
