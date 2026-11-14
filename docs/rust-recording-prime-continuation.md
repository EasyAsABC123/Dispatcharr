# Recording continuation after a settled prime-write rejection

Original catches the captured PostgreSQL `P0001` error while first marking an
owned recording as active, continues capture/remux, persists completed metadata
and returns SUCCESS with a null result. Rust previously stopped this path.

The scoped Rust library now continues only when the prime mutation received a
database `P0001` rejection and the existing supervised operation subsequently
completed its same-connection ping/Ready fence and explicit close. Other stages,
codes, transport errors and failed settlement retain their operational-error
behavior. There is no trigger-name or fixture-message match in production.

The rejected write did not persist `started_at`, `file_name`, `file_path` or
`_hls_dir`. The correction preserves that absence, writes permanent URLs while
properties are still scheduled, then merges the observed completion fields.
Audit paths come from validated task-local paths rather than invented database
properties. The normal successful-prime path retains its existing behavior.

One fresh Native test passed with the actual P0001 ErrorResponse followed by a
Ready reply held for 6.2 seconds. Two original-owner samples remained held; media,
metadata mutation and terminal results did not advance during the hold. After
release, capture/remux and whole-video EOF decoding passed, the final property
key set and audit paths matched Source semantics, Original AsyncResult read
SUCCESS/null, and the owner retired. Full default 62-table/59-sequence audits,
53 command receipts, process-generation checks and five fresh cleanup queries
passed. Duration and media-byte equality are not claimed.

The Original reference retains its actual host exit 1 and guest exit 0. A separate
known-error-gated review qualifies body/effects after correcting an event-history
baseline; its overflowing passive monitor remains unqualified. A later pinned
serializer primitive verified all retained raw/decoded event associations without
replaying Original. These do not turn the original controller into a passing run.

The [source checkpoint](../scripts/rust-component-checkpoints/current882/README.md)
binds the runtime, Source-derived golden data and actual validation epochs.
Postclaim uncertainty in other paths still poisons and retains the actor until
owned process exit. Main/default workers, live recovery, arbitrary recording
profiles and full M6 remain open. A longer audio/video constructor is separate
work. Original Dispatcharr authors retain full application and algorithm credit.
