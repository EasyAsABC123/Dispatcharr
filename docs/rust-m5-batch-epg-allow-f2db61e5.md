# Batch Set EPG success header

Original and the exact `f2db61e5` Rust Main package matched the
successful `Allow: POST, OPTIONS` header on both administrator requests.
One request assigned a dummy Guide entry to the selected managed and
manual channels. The next cleared the assignment. Four detail reads per
backend and the starting and ending scoped PostgreSQL state matched.
Cache and queue observations stayed unchanged. All four owned roles
retired.

The a33 comparison retains the prior header difference. This scope
covers the two successful POSTs and their readbacks. Full M0, M5, M7
and performance remain open. The original Dispatcharr authors and
contributors retain full credit.
[Redacted receipt](../scripts/rust-integration/m5-batch-epg-allow-f2db61e5.json)
