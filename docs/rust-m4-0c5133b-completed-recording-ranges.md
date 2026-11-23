# Completed recording full and closed-range reads on 0c5133b

Original and the current Rust image served the same synthetic completed Matroska recording in isolated Compose fixtures. A signed administrator received HTTP 200 for the complete 3,165,955-byte recording and HTTP 206 for the closed range at bytes 262,144 through 458,751. Both backends matched the expected body bytes, content length, content type, range and disposition headers.

The paired run verified that the recording mapping and sequence stayed unchanged and then retired the exact owned recording, storage copies and Compose roles. The physical runner and independent proof exited 0. The [aggregate receipt](../scripts/rust-integration/m4-0c5133b-completed-recording-ranges.json) identifies the exact image and workload.

This qualifies the complete and closed-range recording requests on the current image. Wider DVR/VOD parity, decode, latency and resource usage have separate acceptance gates.

Original Dispatcharr and dependency contributors retain full credit.
