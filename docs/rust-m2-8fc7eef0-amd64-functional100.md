# M2 AMD64 100 signed viewers sharing live MPEG-TS

Pinned Original Dispatcharr and the exact `8fc7eef0a702b57d3c6b20dac171478b38e0196e` Rust package each served 100 concurrent signed viewers of the same synthetic channel. Each viewer made one successful HTTP request, advanced through the five-second overlap and produced a bounded 2 MiB capture. The audit independently rehashed all captures and decoder transcripts. Every capture decoded audio and video and cleared the 25-video-frame fixture threshold.

| Observation during the overlap | Original | Rust |
| --- | ---: | ---: |
| Active authenticated viewers | 100 | 100 |
| Successful audio and video decodes | 100 | 100 |
| Active upstream connections at both checks | 1 | 1 |
| Occupied provider slots at both checks | 1 | 1 |
| Shared origin's cumulative connection count at both checks | 1 | 2 |

Both arms kept the primary provider assignment, with no additional connection during the overlap and advancing bytes from the upstream. The fixture reused the same origin across both arms. Original closed its connection before Rust started, so the cumulative count rose by one when Rust opened its own connection. The harness primed each arm with FFmpeg before measured signed viewers joined. The receipt did not record which request initiated the connection.

All six literal exits were 0. After playback stopped, the owned viewers, upstream connections, provider slot and assignment keys cleared. The raw receipts proved five claimed container IDs and process generations gone and empty project container, network and volume inventories. The fixtures used the private AMD64 Compose engine and dynamically assigned loopback ports. [Sanitized evidence and exact package and receipt hashes](../scripts/rust-integration/m2-8fc7eef0-amd64-functional100.json)

This run covers functional authenticated live TS fanout on the stated package. Earlier measured runs remain separate evidence. Wider client and lifecycle coverage and formal M2 acceptance remain open.

Original Dispatcharr, React, FFmpeg and dependency authors and contributors retain full credit.
