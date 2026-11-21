# Ordinary VOD and timeshift on d426

Pinned Original and the exact Rust package `d426d322b27a` matched
ten paired numeric and calendar VOD seek cases. The 20 requests returned
HTTP 206 with matching upstream seek parameters, range headers and bytes.

The same authenticated client then completed two reads on one issued
VOD session. Each backend served 4,299,455 matching media
bytes per read and decoded audio and video. Session metadata stayed idle
through the first EOF, the provider slot released after each read, and
the session later retired.

A separate default timeshift session used one authenticated HTTP 201
setup and one HTTP 206 read per backend. The 1,880,000
matched bytes supported a strict 3-second
decode with 74 video frames and audio.
Each backend used one ordinary archive upstream request and retired the
session, pool and provider slot.

All three scope reducers and ownership checks passed. This group covers
these media workflows. Wider M4 and comparative performance have
separate gates. [Redacted receipt](../scripts/rust-integration/m4-d426d322-ordinary-media-group.json)

The original Dispatcharr and dependency authors retain full credit.
[Attribution](../ATTRIBUTION.md)
