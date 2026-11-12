# Bounded large-logo spool and process-loss recovery

Original Dispatcharr, Django, Celery, Kombu, Channels, Redis and Rust authors retain code, algorithm and license credit. This is a normal inert scanner/application constructor extension. Main/default intake remains off. It does not qualify the M3U/EPG scanner branches, general concurrent filesystem mutation, or the full scanner/M6 milestone.

The legacy in-memory constructor retains its128-file limit. Explicit `Scanner::new_spooled`, `LogoApplication::prepare_spooled` and `IndexedLogoApplication::prepare_spooled` support a bounded4096-entry profile with the existing64KiB aggregate path,4096-byte path and32-directory limits. A separate managed scratch directory uses at most256KiB of metadata/path spool plus one136-byte control file. Full Source-order enumeration, validation and flush complete before SQL/cache/event effects. Records retain mtime f64 bits, files precede descendant-directory traversal, and processing uses at most32 records per batch. No postdiff sorting or payload-file caching is introduced.

The controlled Source259 case actually enumerates257logos and two ignored files. The second case catches one SQL per-file error while retaining all other ordered effects. The reference is the separately captured public85-check Original worker corpus, `../helper-large/source-helper-large-4.json`. Source creation order is reproduced first; the controller asserts actual input walk order before native execution, then compares rows including IDs/order, every marker byte/TTL, event and untouched input files. A prior fixture that recreated files in walk order changed tmpfs traversal and failed; it remains historical evidence rather than being normalized.

## Exact ownership and recovery

Linux Rust1.90 `File::try_lock` supplies one nonblocking exclusive advisory lock on a stable control inode. No lockfile rename/unlink, cloned descriptor, PID timeout or guessed stale ownership is used. This is process-loss recovery on a managed stable directory, not power-loss/reboot/NFS recovery. The Linux no-follow open flag is scoped to this Linux-only implementation; foreign symlinks are refused. Control and scratch must be regular mode0600 files with one link and the managed directory UID. External writers/renames inside the managed directory are unsupported; this does not claim descriptor-relative hostile-directory security.

The136-byte control holds a version header and two bounded generation slots with complementary bytes. State/generation/UUID/reserved fields and complete directory shape are validated before cleanup. An active UUID is synced before exclusive scratch creation. Recovery deletes only its exact recorded v2 file, including a partial/empty file. Unknown, foreign, v1, symlink, hardlink, mode/UID mismatch, oversized or invalid-control cases refuse without arbitrary cleanup. A missing recorded scratch is a distinct safe state. A partially initialized control has no previous valid state and is refused: bootstrap-crash recovery is explicitly open. The complement scheme is not cryptographic integrity or a power-loss atomicity claim.

After physical scratch close/unlink, idle state is synced. The control remains for future lock exclusion; an idle control is expected managed state, not a leaked scratch. Cleanup/control failure remains operational and first-scan false. Caller, admission, actual descriptor and directory lock remain owned through SQL/rollback/close, cache/sink/helper publication receipts and cleanup. A lock cannot make separate applications' process-local first-scan flags globally equivalent; multiple independent scanner applications remain outside the admitted domain.

The10 unit/filesystem/process controls include actual independent process locks, four SIGKILL windows (partial scratch, active marker before create, after unlink and partial idle-state write), foreign-file retention, codec corruption and generation boundaries. The task controller additionally publishes using actual Original Celery, blocks a real native logo SQL operation after earlyACK, kills the native process, observes no terminal result plus its recorded scratch, then starts a fresh UID65532 native application. The fresh task recovers only that recorded file and completes real row/marker effects; Original AsyncResult reads normal null. The lost task stays PENDING during the bounded observation, which does not prove visibility-timeout non-replay. UID65532 executes in the pinned Original fixture runtime; this is not a packaged image50/read-only-root application claim.

All filesystem/SQL/cache/sink tests use disposable owned data. Helper preflight SQL-before-ACK remains the previously documented difference from Original earlyACK; this extension does not relabel it as timing parity. M3U/EPG import, unknown task forms and first-scan helper domains beyond the already qualified bound remain unsupported.

## Portable reproduction

Requires pinned Rust1.90/Cargo, cached dependencies, Docker Compose and the repository's pinned Original image. Both supplied peers are test-profile fixture executables; there is no resource-performance or optimized-ELF transfer claim.

```sh
RECOVERY=scripts/rust-job-contracts/scanner-logo/spool-recovery
python3 "$RECOVERY/test_reconstruct.py" --repo .
python3 "$RECOVERY/reconstruct.py" --repo . --output artifacts/rust/recovery-repro-source
python3 "$RECOVERY/test_build_peer.py" --source artifacts/rust/recovery-repro-source
python3 "$RECOVERY/test_runner.py"
python3 "$RECOVERY/build_peer.py" --source artifacts/rust/recovery-repro-source --target artifacts/rust/target-recovery-repro --output artifacts/rust/recovery-repro-build
artifacts/rust/recovery-repro-build/dispatcharr_job_transport logo_scanner::spool:: --nocapture
python3 "$RECOVERY/run_pair.py" --repo . --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/recovery-repro-build/dispatcharr_job_transport --reference scripts/rust-job-contracts/scanner-logo/helper-large/source-helper-large-4.json --output artifacts/rust/recovery-repro-service
python3 "$RECOVERY/run_pair.py" --repo . --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/recovery-repro-build/dispatcharr_job_transport --reference scripts/rust-job-contracts/scanner-logo/helper-large/source-helper-large-4.json --task --output artifacts/rust/recovery-repro-task
```

Reconstruction uses immutable committed base208598ee and ten exact packaged runtime overlays, each bound by SHA256 and Git blob identifier. The entire770-input map, base overlap and compile-time fixtures are verified. Packaged payloads make historical registration bytes reproducible even after future canonical merges; there is no private artifact or mutable-tree fallback. Do not replace newer canonical source wholesale with this historical reconstruction. The runner verifies31 committed support file hashes before starting a fixture and chmods only its disposable ELF copy0755 for UID65532.

See the accompanying summary for executed source/ELF/tools, same-executable helper/legacy/ordinary/notification/expiry regressions, portable controls and fresh-build actual replay. Failed attempts remain: unavailable optional psycopg2 import before hardloss controls, mode0700 ELF refusing UID65532 after the real kill witness, initial strict formatting lint, initial default-test sandbox socket denial and one wrong regression runner filename. All were corrected without changing the frozen runtime behavior or loosening acceptance. Opaque native traceback diagnostics remain distinct from Python traceback bytes.

## Executed final validation

The qualified770 map passed strict whole-workspace/all-target Clippy,486 default tests (688 ignored), non-test workspace checking and all10 scratch/process controls. Original-ELF results are service544+four physical, task/recovery560, spooled helper80+four existing helper controls, old scanner task85+five, ordinary/stat5135+four, legacy families58, notification87+five and expiry83+15. The helper physical controls use their unchanged legacy memory-scanner harness; the spooled service's separate four tests establish spooled physical SQL/cache/Channels/lost-reply ownership. These scopes are not merged into a fabricated all-spooled helper timing claim.

The public reconstruction11, builder5 and runner2 negative/closure controls passed. The fresh portable build reconstructed every770 input and produced job ELF4044685f18450d6007ccf93f14d178854dc2e552b20501f1782b1ea585051a7b. On that distinct executable,10 filesystem/process controls, actual public task/recovery560 and service544+four physical controls passed. There is no binary/performance transfer claim. Twelve successful/failed owned projects were independently checked absent, and all fixture default-database audits passed.

To replay the exact spooled helper configuration against either rebuilt peer:

```sh
python3 "$RECOVERY/helper/run_pair.py" --repo . --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/recovery-repro-build/dispatcharr_job_transport --task-reference scripts/rust-job-contracts/scanner-logo/index-helper/source-helper-reference-4.json --resources --output artifacts/rust/recovery-repro-helper
```

The helper tools are exact successful executed bytes; they explicitly pass spooled=true to the prepared normal application peer. Other unchanged public regression runner paths and hashes are listed in regression-tool-bindings.json.
