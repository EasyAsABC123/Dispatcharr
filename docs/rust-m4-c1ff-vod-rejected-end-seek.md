# VOD same-session rejected end seek

Original Dispatcharr and dependency authors retain full credit.

The isolated Compose test ran pinned Original and the existing published
Rust application `c1ff40056abc819d477ffa8eb82a45b1ebba38d8` at
32,768 and 131,072 bytes per second. Both returned HTTP 416 for a seek at
the known end of the same VOD object, opened no new provider request for
the rejected seek, decoded 75 frames on the independent surviving reader,
and returned 200 on the ordinary late request. Both cohorts verified
teardown of all owned roles and fixture resources.

The earliest public state after the rejection reported two provider slots
on both backends. Original kept the departing session active with a request
count of two. Rust reported an inactive departing session and a count of
three. The instrumented Original iterator established that rejection came
before its pending read progressed. The provider observations do not
establish the Rust local cancellation point.

The preflight, physical controller, and strict diagnostic reducer each
exited zero. Their success records the observed difference and verifies the
controls and cleanup. M4 parity and performance remain open for this path.

[Exact candidate and redacted two-rate evidence](../scripts/rust-integration/m4-c1ff-vod-rejected-end-seek.json)
