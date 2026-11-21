# M1–M3 ordinary viewing on d426d322

All six viewing workflows on packaged Rust `d426d322b27a197aaaaa057ef73dfb53c516c452` passed their physical checks and independent cleanup. The MPEG-TS concurrency, HLS and fMP4 owners ran paired Original and Rust phases. The sharing, retention and pacing owners exercised packaged Rust Main across two workers.

- **M1 sharing and retained history.** Two authenticated viewers shared one upstream and one provider slot. A viewer continued after the other closed, and normal operator stop ended both viewers in the separate stop case. A ten-channel fixture completed 100 bounded late-history reads with a concurrency cap of 32, matching retained prefixes without extra upstream connections. A separate test verified a continuous and a slower reader both received useful audio/video.
- **M2 simultaneous viewers.** Each backend served 100 authenticated viewers on one channel through a common overlap window. All 100 made useful byte progress and decoded audio/video on each backend while the origin count remained one. After ordinary closure the origin retired and five exact fixture roles settled.
- **M3 shared output.** Two authenticated HLS viewers per backend shared one origin and one profile process, matched the advertised segment bytes and decoded real audio/video. Two authenticated fMP4 viewers per backend shared one origin, transcode and remux, matched complete fragments and decoded real audio/video. The fMP4 survivor made progress after normal peer close.

The four retained-history samples and the pacing observations concern local payload and canonical history limits. In-flight HTTP buffers, total process memory and CPU sampling remain outside these recorded measurements. The 100 late-history requests and the separate 100 simultaneous streams answer different workload questions. Formal acceptance and comparative performance remain open.

[Machine-readable receipt](../scripts/rust-integration/m1-m3-d426d322-ordinary-viewing.json)

Original Dispatcharr and dependency authors and contributors retain full credit.
