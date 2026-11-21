# Comskip replacement on the native ARM64 donor ABI

The pinned Comskip source at `a140b6ac8bc8f596729e9052819affc779c3b377` compiled on native ARM64 with GCC 15.2.0 and the original Dispatcharr compatibility edit. The network-disabled build verified and added the 51 signed Ubuntu archive packages while preserving all 426 installed donor package versions. The executable linked the actual donor's distro `libavformat.so.62`, `libavcodec.so.62`, `libavutil.so.60` and `libswscale.so.9`. The resulting binary SHA-256 is `9cd2bd32020bb84f27c9a0b187a408610d861137b1eb59a081ce24e7bf2f7dbb`. [Successful hosted validation](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44867437182/job/136734149679) · [Exact solver inputs](rust-m10-native-arm64-comskip-toolchain-plan.md)

The smoke stage inherited all 17 Original donor layers and kept its package inventory unchanged. The replacement ran as UID 65532 on a read-only filesystem without network. It preserved the generated 8.0196-second MPEG-TS input, returned the expected help status 2 and no-commercials status 1, and produced an empty EDL. The observed closure resolved 138 libraries with 126 installed package notices and zero libraries missing a package notice. The exact container and temporary image retired. [Redacted receipt](../scripts/rust-integration/m10-native-arm64-comskip-donor-abi.json)

The first hosted attempt verified all 51 package archives and exited 1 during local Apt installation because Apt produced a relative archive path. The successful retry supplied the same exact archives with isolated Apt state, empty source lists and a network-disabled build. [Retained first attempt](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44867122086)

This result qualifies the source-built replacement and synthetic fixture on the public Original donor ABI. The historical detector's corresponding source, the replacement's distribution source and notice closure, positive-mark media, and the full ARM64 Rust package remain open.

The original Dispatcharr authors, Erik Kaashoek and Comskip contributors, and FFmpeg, Ubuntu and dependency authors retain full credit. [Attribution](../ATTRIBUTION.md)
