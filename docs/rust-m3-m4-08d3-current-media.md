# Current media checks on Rust 08d3b455

Original Dispatcharr authors and contributors retain full application, protocol and UI credit. FFmpeg and other dependency authors retain full credit for their work. See [upstream attribution](../ATTRIBUTION.md).

- **M4 active catch-up.** Original and the published Rust image returned 201 for the owned catch-up session and 206 for the same 3,760,000-byte range. Pause and resume returned 204, advanced the playhead, and kept one provider reservation and one upstream Range request. The full range matched by digest, with three seconds of audio and video decoding. Both arms matched the retained Original reference, and the four owned Compose roles and all session resources retired.
- **M3 planned fMP4 handoff.** Two native Main instances used the same published Rust image. After the lead retired gracefully and released its provider slot and media processes, a fresh connection on the ready peer opened the same channel and profile. Both readers received six complete fMP4 fragments and decoded 74 H.264 frames with AAC audio. The origin served two sequential connections with a peak of one. The peer and all owned Compose resources retired.

The grouped owner and both physical scopes exited 0. fMP4 Original handoff, existing-socket continuity, unplanned failover, application performance, and the remaining M3 and M4 workflows have separate gates. [Machine-readable evidence](../scripts/rust-integration/m3-m4-08d3-current-media.json)
