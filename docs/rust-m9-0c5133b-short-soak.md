# M9 current-image two-minute soak qualification

The ordinary AMD64 package for Rust application
`0c5133b62dc9f2df0900f2228e25b75baecb6754` passed its two-minute
Compose qualification. The isolated fixture sustained one H.264/AAC
stream, completed 20 audio/video decodes, and processed the two
scheduled XMLTV updates at five and 60 seconds.

Preflight, execution, independent proof, and the wrapper all exited 0.
The independent cleanup check verified all five owned service generations
retired, with empty container, network, and volume inventories. The
receipt permits the separate six-minute qualification. The two-hour
run and release performance acceptance remain open.

Original Dispatcharr, its React frontend, and its dependencies retain
their existing attribution and licenses.

[Machine-readable receipt](../scripts/rust-integration/m9-0c5133b-short-soak.json)
