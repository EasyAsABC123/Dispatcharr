# XC movie and episode playback

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit. The React frontend continues to use their
implementation.

The exact candidate revision is `efa55e136a4e2f20b985fadfd51c10caf0995f36`.
It registers the Original XC movie and episode URLs through the Rust VOD
engine. The route authenticates and authorizes every follow, retains the
Original XC same-path session redirect, and uses the shared session, Range
and provider machinery for playback. The separate UUID entry keeps the
existing STREAMS policy.

The first revision `2b9c588cbfb48e91ff462e6b08d87c4b5ffd9264` failed
compilation because the inner timeout borrowed Axum's unsynchronized request
body. The retry isolates the HTTP method before entering that future.
Formatting and strict all-target Clippy passed. The VOD and XC crates
passed 29 and 63 tests, with 21 and 25 external tests ignored. Main passed 41
tests with one external test ignored.

The exact local Docker build passed and preserved all 497 React, license
and attribution hashes. Both backends completed all 13 Compose requests
and cleaned up their owned resources. Twelve responses matched the selected
wire checks, including the same-path redirects, three exact Range responses,
fresh authorization on each follow and inactive-provider denials.

The separate UUID proxy correctly denied the request with HTTP 403 on both
backends. Its Native JSON omitted the space after the field separator, so
the byte-for-byte fixture exited 1. The response encoding correction
requires a successor replay. Full XC acceptance and new performance
measurements remain open.

[Paired Compose evidence](../scripts/rust-integration/milestone-batch101/xc-first.json)

[Candidate100 comparison](rust-milestone-batch100.md) ·
[Milestone ledger](rust-milestones.md)
