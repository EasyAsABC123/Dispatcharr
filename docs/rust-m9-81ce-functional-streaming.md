# Same-image MPEG-TS functional checks

Original Dispatcharr authors and contributors retain full application, workflow and algorithm credit.

The 81ce production package targeted the existing three-node MPEG-TS chain.
Each dispatched workflow used its own isolated Compose fixture. The
coordinator exited 2 with 124 blocked, 3 passed.
It independently verified the exact inventory after each dispatched workflow.

| Required coordinator node | Result |
| --- | --- |
| `package/sharedTS18` | Passed |
| `package/sharedTS6-all-down` | Passed |
| `package/retention100` | Passed |

The two-worker run passed all 24 checks across shared demand, global stop and owner recovery. Two readers shared one upstream connection and one provider slot per case. Six captures decoded audio and video.

The delayed restart passed all 14 checks. Both worker processes first exited, leaving the input index after owner expiry. Restart removed the index and released the provider slot without opening another upstream connection. Both captures decoded audio and video.

The ten-channel retained-history run made four functional observations through 75 seconds. All 100 late readers matched a recorded historical prefix with no additional upstream connection. Twenty captures decoded audio and video. The sampled payloads stayed within the 4 MiB local cap and 16 MiB authoritative cap per channel.

Both standalone backup owner flags were absent in each workflow. The e765
results remain with that image. The 81ce observations
cover functional MPEG-TS checks and payload bounds. Comparative CPU and memory,
HLS, and full M9 acceptance remain separate.

[Redacted scoped receipt](../scripts/rust-integration/milestone-batch113/81ce-functional-mpeg-ts.json)
