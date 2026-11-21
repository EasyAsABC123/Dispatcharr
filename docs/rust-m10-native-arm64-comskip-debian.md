# Standalone Debian ARM64 Comskip replacement

The native hosted run rebuilt the pinned Comskip source at `a140b6ac8bc8f596729e9052819affc779c3b377` using 51 reviewed signed-index package inputs. The binary matched the prior ARM64 collector intake hash. In pinned Debian Bookworm, the UID 65532 guest verified 271 files, all 138 private library paths, and 126 installed package notice records. The source archive, original recipe, detector configuration, and upstream attribution remained part of the verified closure. [Hosted run](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44877868206) · [Earlier collector intake](rust-m10-native-arm64-comskip-collector.md)

The standalone wrapper processed an 8.0196-second owned MPEG-TS fixture. FFmpeg decoded its audio and video. Comskip returned its expected no-commercials exit 1 and wrote an empty EDL. The input hash remained unchanged. Build, guest, and controller exited 0, and the temporary image and container retired.

This receipt covers the separately source-built native replacement. Historical donor-binary provenance, integrated ARM64 application behavior, and redistribution remain separate. Original Dispatcharr, Comskip, FFmpeg, Ubuntu, Debian and upstream contributors retain full credit. [Machine-readable evidence](../scripts/rust-integration/m10-native-arm64-comskip-debian.json)
