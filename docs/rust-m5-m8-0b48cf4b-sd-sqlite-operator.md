# Schedules Direct activation and historical SQLite copy operator

Exact Rust application `0b48cf4b66d598f1ce470eb4d5f5adf95a4100fc` was built with 2,133
application inputs. The 497 React, license, and attribution inputs remained
byte-identical to the receiving parent.

The pinned Original and Rust each used the ordinary Schedules Direct detail
PATCH in two isolated Docker Compose runs. Deactivation disabled the existing
24-hour task and cleared its last-run time. Reactivation enabled the same task,
kept its execution count and null last-run time, and returned idle. Both
directions used only the `is_active` field, left the comparison source unchanged,
and caused no immediate provider request or refresh. The same ordinary Rust
image qualified both directions and passed exact teardown.

A separate opt-in package built from the same source archive passed the
historical SQLite copy run. It upgraded a detached copy from 132 to 184
migration records and verified 62 application tables. The input bytes stayed
identical. Unknown history and a staged collision each returned the expected
exit 1 with a typed refusal. The independent check verified the ordinary and
opt-in Main binaries match byte for byte and verified the installed attribution
and notices. All five operator cases used networkless Compose, UID 65532, and
exact teardown.

The full workspace suite passed 1,543 tests across 193 targets. Another 814
fixture-dependent tests were ignored. The first suite invocation hit a
sandbox loopback restriction. The same frozen source passed with local
test sockets enabled. Workspace format and strict Clippy also passed.

The ordinary package manifest is `5648befe8ceef6da645bd501b0e53ccc2847225a832a46efddbc09b16f853567`.
The reusable operator receipt has SHA-256 `2f6b5f14a0ac0278d315f8af81f759a8d50d2752cf495e2f143bb564ff3a9087`.
The first package wrapper exited 1 before Docker because its configured
environment was missing. The corrected wrapper and its verified workloads
ran against the same application commit. The first opt-in wrapper built the
image and ran default help successfully, then rejected the daemon's lowercase
absence message. The five cases were replayed on the same retained images
with typed container inspection and exact project inventory checks. Live
SQLite serving, full milestone
acceptance, and comparative performance remain open. [Redacted receipt](../scripts/rust-integration/m5-m8-0b48cf4b-sd-sqlite-operator.json)

Original Dispatcharr authors and contributors, including the authors of the
integrations and their dependencies, retain full credit.
