# Native managed startup logs

The original Dispatcharr authors and contributors retain full application,
collector and React credit. [Attribution](../ATTRIBUTION.md)

The Native app stays PID 1 under UID 65532. Its managed writer captures
the normal console streams while forwarding their original bytes. A bounded
2 MiB queue and separate file writer keep storage work off the server path.
The writer uses the existing role name and collector settings. Main retains
its existing termination and exit status handling.

The first exact Native package created a fresh managed log containing its
real readiness record. The stock admin list reported an active collector
and the correct file metadata. The tail returned the complete prefix of
the same file through its exact cursor. The console showed the original
readiness message and SIGTERM exited zero. Pinned Original separately
verified the same list and tail behavior on its own startup log. The fixture
compares each backend against its own file and output.
[Original active-log receipt](../scripts/rust-integration/milestone-batch92/stock-original-active-logs.json)

Independent review found two file handling gaps in that first Native
candidate. Its successor checks the configured cap before appending a batch
and stops file writes when the required rotation fails. A confirmed same-role
standby drops its file backlog before later taking ownership. The new Rust
controls verify both cases.

The fresh startup scope, rotation failure invariant and standby invariant are
recorded separately. Runtime settings transitions, rotation under load,
replica behavior, resource measurements and full M8 remain open.

The corrected shared image repeated the fresh-boot check. All seven Compose
phases exited zero. It verified the managed file, authenticated list and
tail, exact console readiness line and clean SIGTERM, with all four owned
roles retired. The test used the ordinary startup configuration. The
separately staged Version startup option remained disabled.
[Native verification receipt](../scripts/rust-integration/milestone-batch113/native-managed-logs.json)

## Stored logs on a read-only mount

The exact corrected `c74ea312bdc7` package passed all 29 System Logs cases
against pinned Original. Both backends mounted the same pre-populated log
directory read-only. The eight listing, 13 cursor tail and eight download
cases matched in status, response body and headers, including allowed
and denied roles, two Unicode attachment encodings and the 24 MiB tail
view cap. Each of the 58 requests left its fixture snapshot unchanged.
The listing returned the expected inactive collector state on the read-only
mount. The 18 foundation checks also passed.

The controller exited 0 with both EOFs and its process group settled.
Teardown independently verified the absence of all four owned containers
and their original host processes. Project container, network and volume
inventories were empty. Active rotation, settings transitions and full M8
acceptance retain their separate gates.
[Read-only System Logs receipt](../scripts/rust-integration/milestone-batch113/read-only-system-logs.json)
