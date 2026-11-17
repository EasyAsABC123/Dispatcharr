# Candidate57: result connection recovery and bounded provider writes

Original Dispatcharr and dependency authors retain full application, protocol,
model, task and algorithm/code credit. React and the original license remain.

Commit `6ca3fcd63f9d7c838b1a462b181e675d44ef3c5c` combines bounded account
create/update/delete, profile create/delete and EPG detail writes with a repair
for result sockets closed after Redis's idle timeout. These implementations
cover the declared positive Source domains; they do not establish complete
backend compatibility or close a migration milestone.

[SDK95](../scripts/rust-integration/milestone-batch57/SDK/index.json) passes
formatting, workspace check, strict Clippy and workspace tests: **1,208 passed,
zero failed and 789 ignored**, across 180 result blocks. All 1,455 delivered
Rust inputs match the tested snapshot and published commit; the SDK includes
1,461 inputs in total. The [SDK history](../scripts/rust-integration/milestone-batch57/SDK/history.json)
preserves actual compilation failures in SDK91, strict lint failures in SDK92–93, and
SDK94's earlier all-four pass. SDK94's green result did not establish that the
new physical reconnect test worked.

Candidate56's focused long-recording diagnostic observed an immediate physical
`redis_disconnect` when the parent stored its result. It was not a logical
command deadline. Independent read-only inspection observed Redis idle timeout
300 seconds. Pinned installed Celery 5.6.3 reads current metadata before storing;
redis-py 6.4.0's pool checks and reconnects an already closed socket before sending
a command. Its library also has a PING/PONG health mechanism; this evidence does
not assert a nonzero configured backend health-check interval. The compact
[provenance](../scripts/rust-integration/milestone-batch57/reconnect-physical/provenance.json)
preserves fixed-vocabulary observations, installed-source hashes and these limits.

Result writes now make a fixed-response read-only PING before the original
atomic result script. One additional PING is allowed only after a physically
settled dropped-connection error. The result mutation is never retried. Its
existing bounded GET, first-SUCCESS check, caller guard, operation admission and
logical timeout remain. Both preflight and mutation stay in the same owned
continuation; caller cancellation does not release their physical ownership.
No background keepalive or global timeout change was added.

[Physical attempt 1](../scripts/rust-integration/milestone-batch57/reconnect-physical/attempt1/summary.json)
ran all six controls against real Redis: five passed and the new closed-socket
identity test failed before CLIENT KILL. The test assumed the host peer address
survived Docker port translation. A wrapper path-hashing error then prevented
its final result file; the actual failed cargo/outer exits and successful exact
cleanup remain preserved. This attempt is not relabelled a production pass.

[Physical attempt 2](../scripts/rust-integration/milestone-batch57/reconnect-physical/attempt2/summary.json)
passes **all six controls**, with an actual outer exit 0 in **17.695 seconds**.
The test now identifies exactly one newly allocated server client ID carrying
its owned read-only marker and refuses ambiguous identities before exact CLIENT
KILL. It verifies reconnect before one mutation, no replay after a lost mutation
reply, and retained owner/admission while PONG is held beyond the logical deadline.
The three preexisting reconnect controls also pass. Primary failures are saved
before cleanup and path hashing accepts recorded path strings.

The physical run used one existing disposable baseline and the existing shared
Cargo target. All command pipes/process groups settled; one exact container,
one original host process generation and project containers/networks/volumes
were independently absent. No Source business case was replayed. This is a
transport component proof, not a long-recording or full application acceptance.

The [Candidate57 Docker build](../scripts/rust-integration/milestone-batch57/build/index.json)
completed with actual exit 0 in 326.072 seconds. Its image is
`sha256:41342af55684b06fcde31232911c9878326d3b875be4878f942333a116d6871c`.
All 1,455 Rust inputs match SDK95 and the published revision. The
[package checks](../scripts/rust-integration/milestone-batch57/package/inventory.json)
passed all six families; 55 exact containers were independently absent afterward.

The [first Native management workflow](../scripts/rust-integration/milestone-batch57/management-attempt1/summary.json)
failed after account creation returned HTTP 201: its generated refresh task did
not drain within 90 seconds. The broker observer subsequently reached its
20,000-row cap. Read-only diagnosis identifies a missing consumer path for an
unparented STD refresh task; startup tasks had succeeded. This is a production
integration gap, and neither the timeout nor the observer cap has been raised.
All 2,079 bound inputs remained unchanged. The actual exit 1 was consumed;
four exact containers and three original host process generations were independently
absent. Successful cleanup does not turn this failed workflow into acceptance.

The independent 15-context application regression is running against this image.
Its result and the management repair remain pending. Caught SQL faults,
unsupported input domains and concurrency limits remain explicit in the bounded
write ports. M0–M9 remain open. Build/test duration is not application CPU or
memory evidence; no new performance gain is accepted here.
