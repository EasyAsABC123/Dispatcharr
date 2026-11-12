# Bounded literal-URL M3U scanner consumption

Original Dispatcharr and dependency authors retain application, algorithm and licensing credit. Read [SCOPE.md](SCOPE.md) for admission boundaries and retained limitations. This extends an inert normal-library component. It does not enable Main/default intake, complete M6, or claim new performance gains.

The historical component is exactly 788 Rust inputs from Git `98e447186af099dd17496539aec46312f6cf4f62` plus four packaged hash-bound overlays. Three are normal runtime/export paths; the fourth adds a fixture-only peer action calling the same public Store API. The complete map is `source-inputs-788.json` (SHA256 `fc69f7c1fcb1386e478f845e6c364e8514c0c8731bae2027862d14737d476c7a`). Five exact Git-coupled timeshift goldens are included in reconstruction. This historical source map excludes later unrelated notification/header changes; Root's separate combined qualification is not silently projected onto this map.

## Actual qualification

- Native2: 17 observations / 393 checks; 12 native-positive flows and five intact refusals followed by separately labeled actual Original fallback. All 1,070 compared temporal fields were retained.
- Store2: 14 vectors / 100 checks, including actual NULL/exact8192/8193/compressible oversized fields, pre-ACK refusals, post-admission enlargement with caller cancellation and bounded SELECT responses, and the old public uncapped Store behavior.
- Physical3: nine phases / 182 checks: delayed Redis replies, preflight/snapshot SQL errors, real successful DataRow+CommandComplete with held Ready, BEGIN loss, mutation error with settled rollback before its one existing retry, rollback loss, and deferred COMMIT error. The 6.2-second SQL holds are established by the fixed executed sleep and retention assertions; there is no independent raw release-clock duration measurement.
- Adapted historical admission: 14 cases / 178 checks. The two literal URL modes are intentional new positives; other refusals remain. Historical176 remains unchanged.
- Original public Store API: the unchanged integration test passed all nine captured batch steps for create/touch/change/dedup/rollback.
- Independent Root regressions on the same QA4 job ELF: audit634/15, audit physical63/3, capacity25/2, legacy275/8, and adapted historical physical32/2. Exact commands/controllers/support bindings and independent project/default audits are retained in the evidence packet.
- Same-source workspace: non-test check and strict all-target Clippy passed; 566 default tests passed / 703 ignored. An initial sandbox socket-bind failure is retained separately from the escalated successful default run.

Actual semantic fixtures used QA4 job ELF `2715ef8f0f7f29e3e322fac67032cbf70143b2bd8b55de5bb1d1ff899d7f4832`. All disposable projects had unchanged default-database audits and independently empty owned container/network/volume inventories. Native1, Store1, Physical1 and Physical2 remain failed executions. Their separate offline diagnoses/comparisons never relabel those processes as successful. Some inherited raw `status`/docstring labels predate the native controllers; actual process exit, ordered cases/checks, source/ELF receipts and independent reviews determine the reported scope.

## Reproduce from public files

Use Rust/Cargo 1.90 on PATH, the required offline dependency cache, Python3, and the existing disposable integration Docker environment. These tools do not install a toolchain or use private source fallbacks. `reconstruct.py` reads immutable Git history plus the exact packaged payloads, verifies all 788 inputs and the five coupled fixtures, and refuses overlap/corruption. Outputs and Cargo targets must remain outside the frozen source.

```bash
tool=scripts/rust-job-contracts/scanner-logo/m3u-url-consumption
python "$tool/test_reconstruct.py" --repo .
python "$tool/reconstruct.py" --repo . --output artifacts/rust/url-repro/source
python "$tool/build_peer.py" --source artifacts/rust/url-repro/source --target artifacts/rust/target-url-repro-1 --output artifacts/rust/url-repro/build
python "$tool/test_bind_build.py" --build artifacts/rust/url-repro/build
python "$tool/bind_build.py" --build artifacts/rust/url-repro/build --output artifacts/rust/url-repro/tools
```

The public reconstruction and fresh three-peer build were executed successfully. Rebuilt ELF hashes differ from the historical QA4 ELFs and are bound in a new explicit build record; they are not mislabeled as the already executed QA4 fixtures. `bind_build.py` verifies compiler records, full source-map and all three outside ELF digests, then writes new execution receipts. It preserves historical receipts/seals/results and changes only explicit compiler/ELF bindings (the legacy integration runner has one corresponding ELF digest constant). Its fresh binding is not itself a fixture pass.

Run each fixture independently, wait for its cleanup and inspect actual exit/report/default audit before starting another. The successful portable compiler build does not require repeating the Source reference capture.

```bash
python artifacts/rust/url-repro/tools/interop/run_pair.py --repo . --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/url-repro/build/dispatcharr_job_transport --reference scripts/rust-job-contracts/scanner-logo/m3u-url-reference/Source-URL-completed17.json --output artifacts/rust/url-repro/interop
python artifacts/rust/url-repro/tools/store/run_pair.py --repo . --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/url-repro/build/dispatcharr_job_transport --reference scripts/rust-job-contracts/scanner-logo/m3u-url-reference/Source-URL-completed17.json --output artifacts/rust/url-repro/store
python artifacts/rust/url-repro/tools/physical/run_pair.py --repo . --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/url-repro/build/dispatcharr_job_transport --reference scripts/rust-job-contracts/scanner-logo/m3u-url-reference/Source-URL-completed17.json --output artifacts/rust/url-repro/physical
python artifacts/rust/url-repro/tools/legacy-admission/run_pair.py --repo . --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/url-repro/build/dispatcharr_job_transport --reference scripts/rust-job-contracts/scanner-logo/m3u-hash-reference/source-hash-1.json --output artifacts/rust/url-repro/legacy-admission
python artifacts/rust/url-repro/tools/legacy-store/run_pair.py --repo . --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/url-repro/build/import_sql --reference scripts/rust-job-contracts/scanner-logo/m3u-url-reference/Source-URL-completed17.json --output artifacts/rust/url-repro/legacy-store
```

The first three and last commands use the previously captured Source17 JSON as an immutable input; actual Original scanner parents and explicitly labeled fallback workers are fixture participants, not a fourth Source URL reference capture. The legacy public Store regression uses the existing unchanged ORM batch helper, not a new URL oracle. Unsupported settings, mapped rows and oversized/concurrent enlargement are native refusal controls, not invented Original parity for those limits. No copied database, cache or task namespace authorizes access to real provider data.

The adapted historical physical replay is also shipped unchanged except its sealed unsupported setting substitution (`url` to `name`):

```bash
python artifacts/rust/url-repro/tools/legacy-physical/run_pair.py --repo . --support-root scripts/rust-core-perf/support --native-binary artifacts/rust/url-repro/build/dispatcharr_job_transport --reference scripts/rust-job-contracts/scanner-logo/m3u-hash-reference/source-hash-1.json --output artifacts/rust/url-repro/legacy-physical
```

Unchanged audit and legacy controllers remain at their existing public roots (no duplicated common support tree). Root's `evidence/root-url-audit-regression-1/dispatch-plan.json` binds their exact invocation/reference and tool map. The old consumption regression uses `scripts/rust-job-contracts/scanner-logo/m3u-consumption/run_pair.py` with reference `scripts/rust-job-contracts/scanner-logo/m3u-reference/source-m3u-4.json`. Use a new output for each command and the freshly built job peer.

`evidence/raw-archive-index.json` binds deterministic gzip copies to the exact uncompressed ledgers/logs; `gzip -dc` recovers the original bytes. The failed processes and their offline proofs are separate directories. Fresh public build records prove compilation only; no fresh-build functional result or ELF identity is borrowed from the historical QA4 or Root791 executions.
