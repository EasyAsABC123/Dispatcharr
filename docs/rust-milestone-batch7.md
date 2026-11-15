# Candidate7 recording and queue corrections

Original Dispatcharr and dependency authors retain all application, algorithm,
protocol and license credit. The React frontend is unchanged.

Runtime `52e8ebc7e4ce8b92368ec94797d60c0e44678687` passes the complete Rust
format, compilation, strict workspace lint and test batch: **911 passed, zero
failed, 751 fixture-dependent tests ignored**, over 1,186 recorded inputs. All
four commands exit zero with unchanged inputs; every Rust input matches Git.
[Raw results and failed diagnostic attempts](../scripts/rust-integration/milestone-batch7/inventory.json)
are retained. One shared Cargo target was reused.

The experimental selected-worker mode now composes actual Recording HTTP
booking/list/detail/update/stop/extend/delete routes with existing UI/DVR/channel
authorization, SQL and background ownership. Real post-save scheduling publishes
clocked or immediate recording tasks and eligible root-artwork tasks. The engine
stops only DVR clients matching the recording's Source User-Agent; its returned
count means successful stop requests, not proof that media has already exited.

The queue dispatcher now waits before retrying the same unclaimed future or
capacity-blocked head. Real claims/completions and head-advance notifications
can still make progress immediately. A paused-clock test exercises both actual
lane implementations, counting broker peeks and handler calls at the configured
poll boundary. The prior observed high-rate loop remains a failed image6 result;
this SDK check alone is not a paired CPU performance gain.

Source-frame adoption no longer permanently depends on an in-process publication
registry or completed parent. Deleted recordings require an actual absence read;
a queued frame's historical arguments remain distinct from the current row after
an extend/update. The consumer independently rereads the actual Recording and
argument Channel. Collector delete callbacks use a sealed same-transaction task
absence observation for control-only revocation, avoiding a second pool checkout
while the original transaction remains held. Captured constructors remain intact.

SQLx now uses the same IPv6 host normalization for TLS identity as for its TCP
connection. Real certificate tests exercise the IPv6 identity, strict hostname
refusal, verify-ca success and wrong-CA refusal. The regression package explicitly
watches its external library source: an earlier all-zero command run used a
stale test binary without the new IPv6 test and is retained as unqualified.
The final batch verifies that the IPv6, queue-backoff and root/child-artwork
authority tests actually executed by name.

This code still requires grouped production-Main Compose acceptance. Normal
recording file/HLS playback, broader artwork and scheduler domains, full Source
task/role/input coverage, SQLite, physical faults and release gates remain open.
Both experimental worker/scheduler flags remain off by default. No full migration
milestone or new resource gain is claimed. See the
[image6 failures](rust-batch6-functional-observations.md) for the concrete triggers.
