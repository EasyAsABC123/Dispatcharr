# One shared payload for 100 viewers on 0c5133b

The Rust stream runtime's reviewed `hundred_viewers_share_one_payload_without_queues` unit test passed against the exact ordinary `0c5133b` sources. One hundred independent readers referenced the same retained payload allocation. The test checked pointer sharing, retained bytes and reader counts without engine, network or database dependencies.

Locked offline Cargo selected the one reviewed test and exited 0 with one pass and fifteen filtered cases. The private source snapshot matched every Rust input in the verified package before and after execution. The [aggregate receipt](../scripts/rust-integration/m1-0c5133b-bounded-viewer-core.json) identifies the package and literal output.

This confirms the selected bounded-core property on the current revision. M0 executable Original contract coverage remains an M1 prerequisite. Published [ten-viewer playback measurements](rust-m2-9f29dbe-live-xmltv-performance.md) describe a separate product workload and revision.

Original Dispatcharr and dependency contributors retain full credit.
