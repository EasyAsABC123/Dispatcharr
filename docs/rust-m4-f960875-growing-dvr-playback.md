# Growing HLS DVR playback on f960875

The original Dispatcharr authors and contributors retain full credit for the application. See [Attribution](../ATTRIBUTION.md).

Original and Native passed 24 checks against owned, generated DVR media on the exact f960875 package. The synthetic publisher atomically published an initial HLS playlist and a second generation with one appended segment. Both applications served both manifests and both segments through their normal authenticated playback routes, matched the expected bytes for their assigned recording IDs, and refreshed the owned playback heartbeat.

The segment hashes matched across applications. Both concatenated media streams decoded 100 video frames with a required audio track. Preflight, controller and proof all returned 0, and all four scoped roles retired exactly.

The [machine-readable proof](../scripts/rust-integration/m4-f960875-growing-dvr-playback.json) identifies the exact package and observed scope. The producer lifecycle, full M4 parity, and comparative performance remain open.
