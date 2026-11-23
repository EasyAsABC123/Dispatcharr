# ARM64 HDHomeRun active output profile

Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and the signed ordinary ARM64 package for `381a6a157d1f860527906794bb8c63859fcc8176` passed the paired Lappy Compose fixture. A configured HTTP client in each arm followed the named HDHomeRun profile’s advertised lineup URL, received the matching guide 2.5 entry and followed its exact active output profile stream URL. Discovery, lineup, scan status and delivery all returned HTTP 200.

Both applications transcoded the same 320×180 H.264/AAC synthetic origin to 192×108 MPEG-2 video with MP2 audio. Both produced the same 1,316,000-byte sample of 7,000 aligned MPEG-TS packets. Its SHA-256 was `2b3647c111f39cff9d5dcacb493aaec0e48c39b08a8ac4ceb12ddc4f6dd23bb4`. Each strictly decoded 74 video frames and audio with empty decoder stderr. Both origin inputs matched SHA-256 `f360ff56a9721818a986182fe0c4b115284382e9b09ab2c3798c8f5d0751fd56`. Each arm opened one primary upstream connection and retired it. The fallback and eight owned database table projections stayed unchanged in each arm.

The application, probe and owner phases all exited `0`. The independent audit verified absence of all 14 owned containers, the two network IDs and names, and four volumes. The private pair receipt SHA-256 was `01f584336da8eef47ad437735498d5b2a6da3ec37b58aefceee4a231ef02f324`. Output was processed in memory and no media or credentials left the verification host.

This result covers one ordinary software active profile. The earlier 8a inactive-profile pass-through evidence remains a separate fixture. Automatic UDP discovery, physical HDHomeRun hardware, broader profile configurations and full M7 acceptance remain open.

Application and React credit belongs to the original Dispatcharr authors and contributors.
