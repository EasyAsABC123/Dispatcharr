# Current opt-in recording library

The inert Rust DVR library now composes with the current Core, PluginRepo,
streaming and Main fixes. It handles the captured30-second UTC/local-TS recording
context, preserves the Original producer envelope, prepares owned paths after
admission, executes the Source-derived SQL cleanup and metadata order, and owns
FFmpeg children, signals, stderr readers and waits. Observer cancellation does
not cancel an admitted recording. Main and default workers do not register it.

The exact878-input snapshot contains873 Rust files and five coupled inputs.
A full workspace preflight passes formatting, compilation, strict Clippy and804
tests, with744 external-fixture tests ignored. A later test-only diagnostic
change passes its five affected checks and job tests; full defaults were not
repeated on that test-only snapshot. Both receipts are retained in the
[verification packet](../scripts/rust-job-contracts/clocked-recording-current-library-qa/Root-review.json).

Actual current-source physical cases pass for pre-ACK cache and SQL ownership,
recording continuation after observer cancellation, result-reply retention, and
preexisting-owner refusal. SQL-write-error and Channels cases retain their
original host/guest failures; separate known-error-gated comparisons qualify
only retained body/effect ownership after correcting Source-envelope and
counter-meaning mistakes. A first result-target attempt remains unqualified; its
corrected fresh attempt waits the actual end before the unchanged target probe.
No failed process status is relabeled. The current observer-loss recording also
passes separate whole-video EOF decoding; its27.48-second duration is not an
equality or performance claim against Original.

This is a scoped component. Postclaim operational uncertainty deliberately
closes and poisons its single-slot actor, retaining the original owner, actor and
transcript through a self-reference until OWNED PROCESS EXIT. A live uncertain
actor is not drained or recoverable. Repeated replacement in a long-running
process is not qualified. Filesystem use requires an exclusive stable namespace;
absolute FFmpeg paths do not provide protection against hostile concurrent
renames. OS waits and reader joins retain owners without a universal latency
bound. The fixture's180-second task budget is not a general production bound;
remux has its own30-minute deadline.

General scheduling, offsets, profiles, providers, audio, terminal Source failure
semantics, same-value foreign-key replacement, crash recovery, default-graph
integration, full M6 and worker-inclusive performance remain open. All
Dispatcharr application and algorithm credit belongs to the original authors.
