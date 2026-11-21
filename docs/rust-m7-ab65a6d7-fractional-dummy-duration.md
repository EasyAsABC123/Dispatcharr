# Fractional duration in generated custom Guide programmes

The pinned Original and Rust application `ab65a6d756437260767138a5cdf457818114e2a1` produced
a 5,490-second event from a 91.5-minute programme on each of two channels.
The XMLTV export, Guide Grid, and Xtream short EPG matched across both
backends. The same workflow retained the existing integer-duration
controls and checked stable programme fields and ordering.

The Rust change keeps the original integer path and handles decimal
durations in the existing editor range. It follows the Original's
microsecond rounding for each accumulated interval. Twenty-eight
arithmetic controls, 92 affected Rust tests, format, and strict Clippy
passed. The paired Compose workflow and independent reduction exited 0.
All owned roles and the four Native regex process generations retired.

The packaged manifest is `83b3a67cdf07d24820c3d64084a3a8855206b78699620354b42e503e8aa7a758`.
The proof SHA-256 is `f7bfb792e5a239d04686003baa1869948b704a0e4b82c7326fdf1b0dfd65a970`. Full M7 and comparative performance remain
open. [Redacted receipt](../scripts/rust-integration/m7-ab65a6d7-fractional-dummy-duration.json)

Original Dispatcharr authors, contributors, and dependency authors retain
full credit.
