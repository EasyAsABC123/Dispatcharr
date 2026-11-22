# Original STD provider credential-pool reference

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

The owned Original fixture verified three configured STD accounts. The latest usable account sample was independent of the channel’s assigned media stream. Its typed path took precedence over a saved pair. A second account’s saved pair took precedence over its short sample and shared the first account’s pool. A third account without a saved pair used its short sample and could play from a distinct pool while the shared pool was occupied.

The shared attempt returned 503 with a reported wait of two seconds and made no upstream request. It played after the first viewer released the shared pool. The three successful reads each decoded 50 video and 88 audio frames from an unchanged complete interior window. SQL stayed unchanged, the media origin retired all requests, and exact Compose cleanup passed.

The first run stopped at its decoder check after receiving a synchronized TS prefix. Its teardown passed. The retry required a clean keyframe-to-keyframe window and retained the first result separately. This report is the Original compatibility reference for the Rust implementation. [Machine-readable evidence](../scripts/rust-integration/m2-original-std-sample-pool.json)
