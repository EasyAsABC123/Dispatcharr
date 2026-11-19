# Candidate107 detailed TS diagnostics and remote XMLTV creation

The original Dispatcharr authors and contributors retain full application,
React, workflow and algorithm credit. The 497 frozen React, license and
attribution inputs are byte-identical to the Original baseline.

The exact source at `1ea708bf321ec7348a9f04a7fa5d16668e4d072b` passed Rust
formatting, locked binary checks, focused regressions, and strict Clippy.
The package built successfully as
`sha256:7e7059895df9f87554ce31bf235a760bd47a5649c1518efc87a63b61f0926d8c`.

The M3 detailed administrator TS status check matched six observations per
backend across two replicas. The cases covered invalid bytes, valid UTF-8,
and a valid first buffer followed by invalid bytes. Both phases exited 0
and removed their exact owned Compose resources.

The M6 remote XMLTV creation check used the retained passing Original
run and the fresh Native package. Both returned HTTP 401, 403, 201, 201 and
400 in the same sequence. Committed rows matched, including the idle state
of an inactive new source. Both backends fetched the provider once. Active
creation produced the expected refresh root and Index and Series children.
Both runs and the Native UID check completed with verified cleanup.

The cohort exited 1 after the M4 calendar-range guest rejected its own
calendar query fields before sending an HTTP request. Exact teardown passed.
The matching M3 and M6 changes are published from the tested snapshot.
The corrected M4 fixture uses the same package for its own runtime result.

These results qualify the stated functional cases. Broader milestone
acceptance and the separate performance gates remain open.

[Functional receipts](../scripts/rust-integration/milestone-batch107/functional.json)

## Calendar and alternate-provider VOD ranges

The corrected calendar guest accepted the previously rejected query fields and
reused the exact Candidate107 image. All five Original and five Rust calls
matched on HTTP 206, the 65,536-byte Range body, provider query and Redis
metadata. Both sides performed one provider request per case. The replay
exited 0 and retired its four exact service generations.

The separate Candidate106 capacity case matched the preferred-provider read,
saturation-driven switch to the alternate, HTTP 503 while both slots were
full, and the return to preferred after release. Exact Range bodies and the
four real upstream requests matched. The full-capacity request made no upstream
connection. The controller exited 0 and exact teardown passed.

[VOD range receipts](../scripts/rust-integration/milestone-batch107/vod-ranges.json)
