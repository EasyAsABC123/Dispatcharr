# Profiled HLS after owner loss on 0c5133b

The ordinary Rust package matched the retained Original playlist timeline after the active HLS owner was killed. Two real viewers shared one upstream before the loss. The follower served a complete segment that decoded H.264 video and AAC audio before the signal. The fixture killed the exact owner without an OOM, observed the surviving replica's unchanged process generation and made eleven requests with the existing follower capability.

The first six responses returned HTTP 200. At 45, 55, 65, 75 and 90 seconds, Rust and Original returned `404 Playlist stale`. The [previous Rust capture](rust-m3-9f29dbe-hls-sharing-and-owner-loss.md) returned `410 Session expired` at the final request after removing the capability while the descriptor remained. In the new run, the strict reducer and exact teardown both passed. Redis observations at 85, 87 and 89 seconds used read-only queries between requests.

Both Rust idle paths now share a bounded grace rule for a still-valid, matching profiled capability with a stale descriptor and vacant raw and HLS owners. Live owners, explicit stop, preemption and true expiry retain their existing behavior. Three isolated real Redis checks verified retention at the qualifying age, release beyond the grace window, and the live-owner and stop cases.

A fresh healthy Original/Rust pair on the same package also passed. Each backend used one upstream connection and one FFmpeg profile process for two viewers. Both observed regular segments matched by sequence and bytes, both viewers decoded H.264 and AAC, and the two stop requests returned HTTP 204. Exact teardown passed.

The [package and playback receipt](../scripts/rust-integration/m3-m4-0c5133b-qualified-comparison.json) records the exact build and the independently observed behavior. This gate covers the existing follower capability at this fixed schedule. New admissions, wider failover, cleanup races under simultaneous refresh and comparative performance remain separate acceptance requirements.

Original Dispatcharr, FFmpeg and dependency contributors retain full credit.
