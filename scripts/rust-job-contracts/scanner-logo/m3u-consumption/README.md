# Scanner-owned M3U groups/single consumption

Original Dispatcharr, Django, Celery, Redis, Channels and dependency authors retain code, algorithm and license credit. This inert library component does not start Main or change default intake.

The actual Original scanner publishes both immediate-parented tasks on the ordinary FIFO. The native worker admits only explicit constructor-owned parent/account scopes, checks actual parent result children, performs bounded domain checks before CAS/early ACK, runs the normal library service, and writes typed normal results read by Original AsyncResult. Unlisted, foreign, periodic and ETA frames remain intact. Existing default workers are unchanged.

The eight-case comparison contains **six native domains**: inactive groups, empty active, one-stream local M3U, groups lock busy, single lock busy, and twice-failed stream batch P0001. Missing local file and group-SQL P0001 are **native pre-ACK refusals followed by real Original fallback**, not native failure parity. No frame is deleted to make the queue advance. SQL rows, ordered events, cache-file bytes, output-cache generation/retired chunks, results and input bytes are compared within the reference's explicit projections.

The scope is flat, owned regular STD M3U files <=16KiB/8 entries with printable ASCII title/attributes and qualified HTTP(S) URL text (never fetched). Only `tvg-id`/`group-title` attributes are accepted. Plugins, filters, XC, Connect, autosync, catchup/mapped channels, compressed/special/hardlinked files, general metadata, missing-file and group-SQL failure metadata remain unsupported. Parsed filesystem/cache/SQL inputs are bounded. Elapsed time is injected: the executed fixtures use explicit 0.0s, matching the Source reference; this is not evidence for all timing/formatting domains.

**Audit history is deliberately nontrimming in this checkpoint.** Before ACK the single task accepts absent/null/0 =>100 or integer1..10000 `max_system_events`; unsupported scalar types/ranges refuse. Existing history >= the limit refuses before ACK. A concurrent crossing after admission is operational, never fabricated normal metadata. Full steady-state history, Source cutoff timestamp ties, deletion ordering and caught audit failures require the next actual Source/native gate. No general concurrency or audit-retention parity is claimed.

The service requires a dedicated max-one pool and the injected AuthService must share that exact pool. Raw SQL exits await trailing ReadyForQuery/ping/close settlement; the physical test holds the real server P0001 reply's trailing Ready for6.2s beyond the existing5s acquire timeout, without rerunning application SQL. Caller/admission/shutdown remain held. Five other actual Redis controls hold completed parent read/CAS/Channels publication/result/cache increment replies across caller abort. Normal drain order is workers, service, then shared ports.

Reproduce from a clone containing base4fdfc1c4154f8f72ff5ccff5928ef02fe1617cfb and the pinned Original commit1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430:

```sh
python3 scripts/rust-job-contracts/scanner-logo/m3u-consumption/test_reconstruct.py --repo .
python3 scripts/rust-job-contracts/scanner-logo/m3u-consumption/reconstruct.py --repo . --output artifacts/rust/m3u-rerun/source
python3 scripts/rust-job-contracts/scanner-logo/m3u-consumption/test_build_peer.py --source artifacts/rust/m3u-rerun/source
python3 scripts/rust-job-contracts/scanner-logo/m3u-consumption/build_peer.py --source artifacts/rust/m3u-rerun/source --target artifacts/rust/target-m3u-rerun --output artifacts/rust/m3u-rerun/build
python3 scripts/rust-job-contracts/scanner-logo/m3u-consumption/test_acceptance.py
python3 scripts/rust-job-contracts/scanner-logo/m3u-consumption/run_pair.py --repo . --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/m3u-rerun/build/dispatcharr_job_transport --reference scripts/rust-job-contracts/scanner-logo/m3u-reference/source-m3u-4.json --output artifacts/rust/m3u-rerun/interop
```

Run the analogous `sql-physical/run_pair.py` and `reply-physical/run_pair.py` with the same arguments and fresh output paths; each has its own `test_acceptance.py`. All use copied fixture databases, Redis8/9 and owned cache0, internal-only networking, audit the default database, and remove their Compose projects. Support bytes are verified against `support-inputs.json`; no private source/support fallback exists. Reconstruction verifies every782 base path and all11 packaged overlays, producing exactly788 paths. New builds intentionally have independent ELF hashes; no performance transfer is claimed.

Retained failures include two controller setup mistakes, wrong Connect table-label preflight, bigint RLS setup mismatch, a NOSCRIPT observer mismatch, sandbox loopback denial, an incorrect single-case reorder negative, the Source-event absolute-count observer mistake, and initial public-control invocation errors. They remain distinct from the successful source/runtime bindings. Historical Source/reference code and result scope strings are preserved; the curated report states the actual roles. This is neither whole scanner nor whole M6 acceptance.
