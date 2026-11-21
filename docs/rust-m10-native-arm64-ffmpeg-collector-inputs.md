# Native ARM64 FFmpeg and ffprobe collector inputs

A native non-root probe on the exact Original ARM64 image identified both shipped `9.0.2-Dispatcharr` tools as AArch64 ELF executables. It verified the 17-layer image, observed their 55 shared-library inputs and recorded 51 installed package notices. The probe and controller exited 0, and the exact container retired. [Hosted result](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44869391482/job/136740265157) · [Digest-bound inventory](../scripts/rust-integration/m10-native-arm64-ffmpeg-collector-inputs.json)

Every observed shared library had an installed package notice. The two custom tool executables had no installed package owner or notice. The inventory records FFmpeg's reported license hash and the exact executable hashes. Corresponding source and the historical FFmpeg builder's raw OCI bytes still need verification. The standalone wrapped Debian runtime, full ARM64 package and distribution remain open. [Original donor and builder provenance](rust-m10-native-arm64-comskip-donor-inventory.md)

The original Dispatcharr authors, FFmpeg developers and dependency authors retain full credit. [Attribution](../ATTRIBUTION.md)
