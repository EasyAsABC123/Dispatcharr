# HLS owner loss and VOD session boundaries

Original Dispatcharr authors and contributors retain full application credit.

The exact Candidate110 Compose replay matched the retained Original HLS
playlist response through the 75-second observation. At 90 seconds, Original
returned HTTP 404 with the stale playlist marker. Rust returned HTTP 410
with the expired session marker. No upstream reopened and no new owner took
over. The controller exited 1 and all five scoped generations retired.

The VOD collision fixture matched both same-session rejoin trials across
Original and Candidate111. In each fresh-session admission trial, Original
returned 503, while Rust returned 200 and opened a fourth upstream request.
The raw observations showed earlier slot release on Rust and a fresh
explicit session being associated with the idle session. The stats observer
read client IDs from the wrong level of the response, so the live stats
projection remains unqualified. Twelve decoder checks passed and the
controller preserved its exit 1.

The HLS terminal reply and the VOD session identity and capacity timing gates
remain open.

[Bounded comparison evidence](../scripts/rust-integration/milestone-batch112/live-session-boundaries.json)
