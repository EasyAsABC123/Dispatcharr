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

The exact local image and 13 cases per backend are in progress. This scope
includes redirects, repeated Range requests and authorization on each
follow. Full XC acceptance and new performance measurements remain open.

[Candidate100 comparison](rust-milestone-batch100.md) ·
[Milestone ledger](rust-milestones.md)
