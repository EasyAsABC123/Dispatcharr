# ARM64 Native ten-viewer shared stream at d2c5e611

Ten distinct signed Native sessions viewed the same MPEG-TS channel. The origin accepted one connection. All 61 samples showed one shared upstream and one provider slot. Each viewer produced useful audio and video, and all twenty decoder processes exited 0. Normal close retired the clients, upstream, provider slot and VLC process. Native exited 0 and the independent audit verified exact Compose teardown.

Original's second run also produced useful audio and video for all ten viewers and retired normally. Its ordinary provider reducer exited 1 after one sample finished 60.866 ms after target against the 50 ms bound. The saved VLC generation scan occupied 56.053 ms of that sample. The evidence leaves the source of that delay unresolved. In the earlier fresh Original run, the longest of 61 samples finished 20.584 ms after target and the arm passed. Both arms of the second run verified exact cleanup.

This matches the ten-viewer sharing requirement for the measured Native package on ARM64. The paired gate and broader M2 acceptance remain open. Original Dispatcharr authors and contributors retain full project and React UI credit.

[Scoped viewer receipt](../scripts/rust-integration/m2-d2c5e611-arm64-native-ten-viewer.json)
