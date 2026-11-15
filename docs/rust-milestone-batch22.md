# Shared output profiles through the raw stream owner

Original Dispatcharr and dependency authors retain full application, media,
algorithm and licensing credit. React remains unchanged.

This candidate joins output profiles across actual SQL selection, Main, TS,
HLS and fMP4. A raw owner dispatches one transform process per channel/profile;
local and remote viewers consume that shared result through Source-compatible
Redis namespaces. Profile demand, reader snapshots and format managers use the
same selected profile. The transform consumes the existing raw session, so its
viewers do not acquire another provider or upstream stream. A bounded owned
subscription receives remote ensure events through the actual Redis transport.

Process stdin/stdout/stderr, writer state and raw ownership remain attached to
the same lifecycle. Profile cleanup precedes raw/provider release. Unknown Redis
mutations retain ownership and fail drain rather than report normal retirement.
The default raw path stays available when no output profile is selected.

Review caught a stop/admission race after an actual Redis await. Admission now
rechecks the captured session under the profile registry lock. The producer
subscribes that captured session directly instead of reopening a historical
source. Stop checks guard claim and process spawn. An ignored physical regression
holds a real successful Redis observation across actual channel stop and requires
zero profile admissions, one origin request and no raw-session resurrection.
That control still requires scheduled execution before acceptance.

Formatting, compilation, strict Clippy and the workspace pass:1,034 tests,
zero failures,764 ignored and176 result blocks. The
[SDK evidence](../scripts/rust-integration/milestone-batch22/inventory.json)
binds1,302 inputs and23 changed source paths. The preceding SDK attempt passed
compilation/tests but failed three strict-Clippy checks. An owned producer
context and equivalent expression cleanup correct those checks; the ownership
race fix was joined before activation.

Docker/package checks, real Redis ownership controls, the Source-first combined
Compose cohort and resource acceptance remain pending. Historical Candidate20
passes do not qualify this candidate. Wider profile edits, exceptional commands,
owner loss/rejoin, enabled plugins, Schedules Direct and other Source gaps remain
open. No full M0–M9 milestone or application performance gain is accepted.
