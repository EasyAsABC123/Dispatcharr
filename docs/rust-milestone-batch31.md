# Actual guide task names

Original Dispatcharr and dependency authors retain application, algorithm and
licensing credit. React remains unchanged.

Main30 exposed a production parser mismatch: actual Source and Native guide
publications use `apps.epg.tasks.fetch_sd_mapped_guide_batch` and
`apps.epg.tasks.fetch_sd_guide_for_epg`. The hook parser accepted a fabricated
`apps.epg.sd_tasks` namespace, rejected the real claimed child before host setup,
and faulted the queue. Live health, queue and SQL observations preserve this
failure; later guide jobs could not reach terminal results.

The two-path correction accepts the actual names and adds a regression through
the real parser using both captured Source wire frames. It retains task/body/
identity constraints and rejects the old namespace and malformed frames.

[SDK41](../scripts/rust-integration/milestone-batch31/checks.json) passes formatting,
workspace check, strict Clippy and workspace tests: **1,099 passed, zero failed,
783 ignored**, across 180 blocks. The snapshot binds 1,408 inputs including 1,402
Rust-tree files. The failed Main30 run and cleanup remain separate evidence.

The [Candidate31 package](../scripts/rust-integration/milestone-batch31/package/inventory.json)
binds runtime commit `ca2e173c797b050dc42ef897501a8fcd5db0f552` to image
`sha256:6cd3617f698006c85bdb7696f3f3b2e441b101aa885d6be9b96ab6635f7ef8bf`.
All six package scripts actually exited zero: archive/assets, crypto, PostgreSQL,
regex, full Python compatibility host and VLC. The archive matches 1,899 Git
files, all 1,402 SDK41 Rust inputs and 495 unchanged React inputs. Installed
assets preserve Original attribution and dependency notices; the production
entrypoint and probes run as UID 65532. The Python audit checks 32,500 hashes,
127 distributions and 425 ELF files, including 328 Original compatibility-library
files. It does not establish enabled-plugin execution. VLC checks useful media
through four Source/candidate package controls. Independent cleanup checked all
53 owned container IDs absent after the six process waits completed.

The first build actually exited one after 607.16 seconds: its runner omitted the
retained BuildKit environment, so the classic builder rejected `COPY --chmod`
after successful Rust compilation. The failed log and wait are retained in the
package evidence. Correcting only the build environment produced the same
immutable revision in 133.22 seconds with actual exit zero. No source change or
additional successful candidate was substituted.

The corrected whole application workflow and performance remain unaccepted.
This is not a fix claim for the earlier RegisteredPlugin shutdown failure and
closes no full milestone.
