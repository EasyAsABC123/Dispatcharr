# Scheduled recording-start attribution after stream fallback

Pinned Original and the exact Rust package at `c6eeee7dd584648da9d8cc3e3ab63bd9a7089d2d` each served two ordinary authenticated MPEG-TS viewers during a finite primary-to-fallback transition. The fallback stayed current. Each arm observed at most one concurrent upstream and four windows with decoded audio and video. [Exact receipt](../scripts/rust-integration/m2-c6eeee7d-recording-start-alias.json)

Each arm booked a future one-off recording. The natural scheduler tick started one scoped recorder as a third viewer while ordinary viewers kept receiving media. One durable audit preceded one matching form delivery, which the owned receiver accepted with HTTP 200. Its stream ID, name, and URL identified the initial assignment while playback remained on the fallback. The recording retired independently and its owned schedule and recording settled.

The Rust arm explicitly enabled Selected Jobs and Beat. The native DVR task recorded successful backend execution, Main exited 0, and both arms verified exact owned cleanup. The first Native attempt exited 1 before booking because the fixture lacked its observer module. The second exited 1 before establishing recording and form proof even though the retained census showed one wrapped recorder. Both failed attempts and their cleanup remain recorded alongside the fresh pass.

This receipt covers one owned finite-media and scheduled-event workflow. M2, the formal workflow roster, and comparative performance remain open. Full credit belongs to the original Dispatcharr authors and dependency contributors. [Original attribution](../ATTRIBUTION.md)
