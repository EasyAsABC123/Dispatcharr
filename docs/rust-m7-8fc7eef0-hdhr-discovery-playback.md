# M7 AMD64 HDHomeRun discovery, lineup and playback

Pinned Original and the exact Rust package `8fc7eef0a702b57d3c6b20dac171478b38e0196e` each served HDHomeRun discovery, the advertised lineup, and the advertised MPEG-TS stream on the owned Compose network. Discovery and lineup returned HTTP 200 with matching headers and projected content. The fixture followed only the two URLs announced for its local device and channel.

Each peer returned HTTP 200 with `video/mp2t`. Both captures contained 7,000 aligned transport packets and decoded 74 H.264 video frames plus AAC audio. Both used one primary upstream connection, left the fallback unchanged, and retired the upstream after closing the viewer.

All five owner and proof exits were 0. The audit verified the exact absence of four owned container and process generations and empty project container, network and volume inventories. Rust exited 0 without an OOM. This covers the tested local HTTP discovery and playback sequence. LAN multicast, physical tuner-client behavior, comparative resources and formal M7 acceptance have separate gates.

Original Dispatcharr, its React frontend, FFmpeg, and dependency authors and contributors retain full credit. [Attribution](../ATTRIBUTION.md) · [Machine-readable proof](../scripts/rust-integration/m7-8fc7eef0-hdhr-discovery-playback.json)
