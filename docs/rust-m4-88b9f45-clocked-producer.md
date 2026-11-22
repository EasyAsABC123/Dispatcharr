# Scheduled DVR producer and playback on 88b9f45

The original Dispatcharr, FFmpeg and dependency authors retain full credit. See [Attribution](../ATTRIBUTION.md).

Each owned Compose arm started one real clocked recording from generated local media, reached the durable recording state, published a playable HLS segment and completed with SUCCESS and a null result. Both produced a 5,693,515-byte MKV that decoded 996 H.264 video frames and 1,866 AAC audio frames. The Rust fixture used explicit Selected/Core ownership.

An independent comparison matched all 2,862 encoded packets in order, including each payload hash, presentation and decoding timestamp, duration and size. The complete files differed by 60 bytes before the first media packet. Authenticated GET returned each backend's exact recording. HEAD matched the expected headers, and the React playback range returned the exact 215-byte slice.

Both physical lanes returned 0 and verified exact teardown. The original reducer returned 1 on an inherited crontab label. A read-only reducer corrected that label check against the same sealed clocked run and passed every recording, playback and cleanup assertion. The [machine-readable evidence](../scripts/rust-integration/m4-88b9f45-clocked-producer.json) preserves both literal results. Default-owner behavior, wider M4 cases and comparative performance remain open.
