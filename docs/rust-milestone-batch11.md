# Candidate11 Source boundary corrections

Original Dispatcharr and dependency authors retain full application, algorithm,
protocol and license credit. React remains unchanged.

This candidate carries the [Candidate10 graph](rust-milestone-batch10.md) plus
five Source-backed boundary corrections before its grouped Compose acceptance.
Formatting, compilation, strict Clippy and the default workspace suite all pass:
**941 passed, zero failed,752 external-fixture tests ignored**, across173 result
blocks. The1217 recorded inputs remained unchanged; Cargo.lock is unchanged.

Channel OPTIONS now returns the captured31-field ChannelSerializer metadata for
all four production routes. Authentication and permission checks precede metadata
and parsing. DRF responses include `Allow: POST, OPTIONS`; unhandled500 responses
do not acquire that header. The shared JSON error formatter locates an unclosed
string's opening quote using Unicode character positions. The actual Source98
observations supply the regression expectations; their full Native HTTP callback
still requires execution.

Normal Source startup performs `BackupsConfig._sync_backup_scheduler`, which
creates the default settings row before synchronizing the periodic task. Looking
only at the scheduler getter missed that write. Candidate11 restores it while
retaining the physical SQL pool lease and uncertain-worker ownership fixes.
This corrects the missing-row behavior described for Candidate10; its immutable
build and SDK evidence remain unchanged.

Scheduled backups now stat the created file after retention. Filename ordering
can remove that new archive after a timezone change or in the presence of a
future-named archive. That case returns a caught failure dictionary rather than
a completed result for a missing file. Exact Source OS exception wording remains
open; Native does not fabricate a Python exception string.

Candidate10 passed packaging verification and was not run through the new full
application acceptance batch before these known code gaps were corrected. No
Native failure capture or application pass is invented for that unexecuted run.
The next coordinated batch uses this corrected candidate, reuses qualified Source
observations, and records all failed, blocked and unexecuted cells explicitly.

No new CPU/memory gain or complete M0–M9 milestone is claimed. Full task/calendar/
role coverage, SQLite, range transport compatibility, soak, restore, rollback and
paired whole-graph performance remain open.
