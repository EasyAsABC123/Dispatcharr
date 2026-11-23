# AMD64 signed 61-second recording on 8fc7eef0

The pinned Original and exact packaged Rust candidate each accepted one signed future recording with HTTP 201. Each schedule used a 60-second lead and a 61-second window. The retained Redis observations matched the four booking arguments to one due DVR publication per arm. Each publication's private execution UUID matched its own successful terminal result and was distinct from the public schedule name. Both recordings finished naturally, and both recorder processes and broker observers retired.

| Observation | Original | Rust |
| --- | ---: | ---: |
| Recorded MKV duration | 46.33 s | 46.33 s |
| MPEG-2 Main video frames, 160×90 at 25 fps | 1,155 | 1,155 |
| MP2 audio frames, mono at 48 kHz | 1,920 | 1,920 |
| Declared whole-file mapped A/V decode exit | 0 | 0 |
| Independent FFmpeg `-xerror` diagnostic exit | 183 | 183 |

Both declared decoders reached end of progress, closed both output streams, and reported matching MPEG-2 damage warnings. The independent strict diagnostic reported corrupt decoded video in each arm. Strict decoding remains open. The two recordings had equal duration and frame counts. Each retained file has its own fingerprint.

Original's web, Beat, Celery, and DVR processes exited 0 with their owned process generations gone. Rust Main exited 0 without OOM. Both Compose projects verified that their exact container and process generations were absent and left no owned containers, networks, or volumes. The package, role exits, fixture, media diagnostics, and cleanup are bound in the evidence. The receipt keeps private broker IDs and signed request material out of the publication.

This scoped pair exercised Linux AMD64 candidate `8fc7eef0a702b57d3c6b20dac171478b38e0196e` against Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. Formal M6 acceptance and equivalent-work performance remain open. The original Dispatcharr, React, FFmpeg, and dependency authors and contributors retain full credit. [Evidence](../scripts/rust-integration/m6-8fc7eef0-recording61-clocked-lineage.json) · [Attribution](../ATTRIBUTION.md)
