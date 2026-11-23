# M6 ARM64 XMLTV and clock qualification

The e7dda7f native ARM64 package completed its direct-origin timing
qualification on Lappy. Original and Rust used the same freshly frozen
guide. In the Rust arm, the signed T+5 request returned 202 and produced a
successful refresh root, ProgrammeIndex child, and Series child. The
fixture verified their lineage, SQL effects, and Redis queue and lock drain.
All five roles recorded 61 samples, with a maximum cross-role span of
9.952 ms against the 50 ms gate. The one direct origin reader completed
the full window. Both arms exited 0 and passed exact teardown.

Earlier runs exposed two fixture problems. The observer counted background
results toward the owned job limit. A later run reached T+5 and failed
a combined programme/index absence assertion before it sent the import.
The corrected fixture staged the guide outside the scanner's watched
directories and kept the source inactive and unscheduled until submission.
It recorded programme and index absence separately. The remaining timed
ten-viewer comparison needs its own reader and performance evidence.

Original Dispatcharr, its React frontend, and its dependencies retain
their existing attribution and licenses.

[Machine-readable receipt](../scripts/rust-integration/m6-e7dda7f-arm64-xmltv-clock-qualified.json)
