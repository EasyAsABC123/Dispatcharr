# ARM64 timing rehearsal and reconnect generation trace

## ARM64 timing rehearsal

A two-service native ARM64 fixture on Lappy replayed a synthetic H.264/AAC stream under the exact Rust `e7dda7f` image. Both counters received the same monotonic anchor and recorded 61 samples. The worst cross-service span was 9.8 ms, and the worst target-to-finish interval was 10.4 ms. Both met the 50 ms threshold. Both guests exited zero and the exact project cleanup passed. The complete ten-viewer workload remains pending. [Machine-readable evidence](../scripts/rust-integration/m2-e7dda7f-arm64-clock-rehearsal.json)

## Native reconnect generation trace

A fresh read-only timeline on ordinary Rust `e7dda7f` reproduced the post-owner-loss stall using a 40-second client read timeout. The fresh request returned HTTP 200, reopened one upstream, sent 295,596 bytes, and timed out before a qualified decode. Raw input progressed while the TS profile owner and index stayed at their initial values. The observer saw retained remux payload and no transform. This locates the stall at the raw-to-profile handoff. A dispatch claim attempt was outside the trace. The diagnostic and exact teardown passed. [Machine-readable evidence](../scripts/rust-integration/m3-e7dda7f-reconnect-generation-timeline.json)
