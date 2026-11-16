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


## Main31 interrupted application evidence

The [actual application capture](../scripts/rust-integration/milestone-batch31/application/inventory.json)
retains Source guide aggregate failure: its nine core SD cases pass, while four
output/custom/dummy cells fail under fixture profile contamination. Source UDP
Proxy sharing, certified media and physical retirement pass; its strict raw
prefix check remains false. Source M8 ends in a key-prefix observer failure;
that failure is not an enabled-plugin parity pass.

Native attempts only the guide context. Its nine core SD cases pass, then the
warm restart's stop observes Main exit1 (no OOM). Diagnostics record the first
PluginRuntime shutdown returning Ok for all seven hosts, followed by a second
entry returning Closed for all seven with11,846 milliseconds remaining and no
unsettled SQL. RegisteredPlugin then reports Operational. Later XML remote HTTP
failures follow removal of Main and are dependent fixture cascades, not
independent download-parity results. The correction needs its own source and
runtime validation.

Root sent SIGINT to the exact controller generation after this known failure.
Cancellation propagated into finally; this was operator-requested interruption,
not natural controller completion. The actual child exits **-2** after953.539
seconds; the wrapper exits1. Both pipes reach EOF and the process group settles.
The Native leaf retains its literal `running` status when interrupted; the other
**nine Native contexts are not run**.

The raw aggregate report records `exact_cleanup_passed:false`, because its
Native result/cleanup append was bypassed by cancellation. A separate
[companion audit](../scripts/rust-integration/milestone-batch31/application/independent-cleanup.json)
verifies both actual fixture cleanup receipts: five exact container IDs absent,
all original process generations absent and host CLI groups settled, with no
cleanup errors. The raw aggregate remains unchanged. No application, full
milestone or performance acceptance follows.
