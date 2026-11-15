# Candidate18 healthy-owner keepalive correction

Original Dispatcharr and dependency authors retain full application, algorithm
and license credit. React is preserved.

The local Source-owner transport now sends keepalive packets only at the buffer
head, after five empty reads and while its actual manager is unhealthy. Empty
reads use Source's backoff, and the keepalive duration cap starts with the first
keepalive and resets after media recovery. Pending history reads retain their
existing ownership; a small cursor observation follows historical repositioning.
The existing Source packet bytes are unchanged.

The real HTTP regression holds a healthy input between media chunks and checks
that the response waits for media rather than emitting a null packet. It fails
against the old engine with exit101 and passes with this correction. Formatting,
compilation, strict Clippy and the full workspace pass:1,008 tests, zero failures,
756 ignored tests and173 result blocks. The
[SDK evidence](../scripts/rust-integration/milestone-batch18/inventory.json)
binds1,261 selected inputs, including1,255 Rust files. The first failed compiler
attempt remains retained locally.

This is the local-owner correction. Remote-owner health requires Source's real
last-data timestamp, two-second health cache, dynamic timeout and retained Redis
read/write/cleanup ports; that separate implementation is prepared for integration.
No remote-owner acceptance is implied by the local HTTP test.

Docker packaging and combined application acceptance remain required. The
[preceding resource capture](rust-batch16-resource-observations.md) remains failed
and descriptive: extra Source fixture controllers biased accounting, and the
old Native transport emitted keepalives during healthy input. The replacement
common controller has passed a real six-child lifecycle peer; its complete
application workflow and fair paired measurements remain pending. No full
M0–M9 milestone or new performance gain is accepted.
