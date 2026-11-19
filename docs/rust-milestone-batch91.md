# Batch guide editing and bounded DVR admission

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit. Candidate91 kept all 497 React, license and
attribution inputs byte-identical to Candidate89.

The existing React bulk channel editor can assign a dummy guide or clear guide
assignments through the Native batch endpoint. It honors admin authorization,
updates only changed channel guide foreign keys, and leaves timestamps and
related rows unchanged. Repeated channel IDs use the last association. Unknown
channels are ignored. The route avoids publishing guide work for dummy or
clear updates, matching Original.

The paired Compose fixture verifies 16 shared HTTP cases on each backend,
including a grant and clear over 103 channels. It also verifies one Original
XMLTV publication and one Native mixed-batch refusal. Native refuses a mixed
dummy/XMLTV batch before applying either change. Full XMLTV and Schedules
Direct batch publication remain open.

The DVR file and HLS routes queue admitted requests for one of the existing
four authentication writers. The wait is FIFO and capped at two seconds.
A disconnected request releases its place in the queue before the owned
SQL/cache/open operation starts. The 32-slot recording admission limit is
unchanged. The exact paired Compose run passed 18 recording checks and
19 foundation checks. Each Compose fixture exited 0, settled its process
group, and verified teardown of all four owned container generations.

The package at `695779244a9dffb477ecb885ebbb19791d3a5021` built successfully.
Formatting and strict all-target Store Clippy passed. The full Store library
run passed 534 tests and left 79 external-fixture tests ignored. Before the
loopback-enabled run, the same revision had eight sandbox permission failures
in tests that bind local sockets. An earlier unshipped revision failed to
compile due to a private parser module import. The acceptance projection
retains both observations.

The preceding [completed DVR benchmark](../scripts/rust-perf/candidate88-dvr/README.md)
qualifies the three one-viewer pairs and records the ten-viewer warmup failure.
The Candidate91 functional result admits a fresh benchmark with the same
workload and budgets. The functional result alone does not establish a new
resource comparison. Full M4 and M5 remain open.

[Exact Docker and test receipts](../scripts/rust-integration/milestone-batch91/acceptance.json)
· [Milestone ledger](rust-milestones.md)
