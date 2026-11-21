# Native ARM64 Comskip rebuild and collector intake

A fresh native ARM64 build without cache reproduced the accepted Comskip binary SHA-256 `9cd2bd32020bb84f27c9a0b187a408610d861137b1eb59a081ce24e7bf2f7dbb`. It verified the same 51 signed package inputs, repeated the generated MPEG-TS smoke, and reproduced the original smoke receipt byte for byte. [Hosted repeat run](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44868579693/job/136737741064) · [First accepted donor-ABI build](rust-m10-native-arm64-comskip-donor-abi.md)

A separate container independently hashed the same binary and passed the six pinned collector inputs, all 138 linked libraries and 126 installed package notices. It ran as UID 65532 with a read-only root and no network. All four controller, build, smoke and collector statuses were zero. Both containers and the ephemeral image retired. [Redacted receipt](../scripts/rust-integration/m10-native-arm64-comskip-collector.json)

The AMD64 collector remains byte-identical. This result covers the standalone ARM64 Comskip intake. Integration into the ARM64 Rust runtime, positive-mark media, and release source and distribution verification remain open.

The original Dispatcharr authors, Erik Kaashoek and Comskip contributors, and FFmpeg, Ubuntu and dependency authors retain full credit. [Attribution](../ATTRIBUTION.md)
