# Source2 clocked Recording producer envelope — inert data only

Original Dispatcharr/Celery/Kombu authors retain application/protocol credit.
Actual Source2 task publication only: whole Source2 remains host1; artwork and
Beat ran, DVR/media/result were never executed. No old failure is relabeled.

The normal public module parses the captured natural dvr four-argument message,
keeps raw bytes unchanged, binds canonical identities, Source SPACE UTC whole-
second dates and periodic name distinct from UUID. Unknown/oversized/duplicate/
unsupported frames refuse. The parsed type always returns UnqualifiedDueExecution;
there is no ACK, registry, worker, TaskLease, FutureRecording, Main or Cargo change.
No normal recording/media/lease/result parity or performance gain is claimed.

Actual qualification: Rust1.90 selected fmt +whole fmt check0, package non-test
check0, strict job all-targets0, seven pure Source-wire tests0 and unchanged legacy
backup-parser refusal test0. Full849 inputs remained exact; tested ELF was copied
after all waits0. Original commands/hashes are historical evidence, not private
paths required to rerun. The test name 'expired_source_wire' denotes the old
scheduled window, not Celery expires: the actual captured expires field is NULL.

Reconstruct using only Git objects and shipped overlays:

    python scripts/rust-job-contracts/clocked-recording-envelope/reconstruct.py --repo "$PWD" --output /fresh/owned/source

Use Rust1.90 and existing cached dependencies; keep HOME/CODEX_HOME unchanged.
Set CARGO_TARGET_DIR and TMPDIR to fresh owned HOME-artifacts paths,
CARGO_BUILD_JOBS=2,CARGO_INCREMENTAL=0,CARGO_PROFILE_DEV_DEBUG=0,
CARGO_PROFILE_TEST_DEBUG=0. From reconstructed source/rust run:

    cargo fmt --all -- --check
    cargo check -p dispatcharr-job-transport --lib --offline --locked
    cargo clippy -p dispatcharr-job-transport --all-targets --offline --locked -- -D warnings
    cargo test -p dispatcharr-job-transport --lib --offline --locked clocked_recording_envelope::tests
    cargo test -p dispatcharr-job-transport --lib --offline --locked worker::tests::create_only_shapes_reject_before_ack

Rebuild results require new receipts and ELF hashes; original ELF bit identity is
not assumed at another source path/compiler environment. No Docker/Source/media
execution is needed for these pure parser checks. Positive Source recording and
separate review must precede any future due-execution adapter.
