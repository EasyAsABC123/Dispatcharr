# Standalone ARM64 Python plugin dependency host

The native hosted run assembled the existing Python collectors and compatibility-source subset into the pinned Debian Bookworm ARM64 runtime. The UID 65532 guest verified 32,359 retained files, 423 AArch64 ELF inputs, notice bytes for 127 distributions, and all 90 Torch notices. Build, guest, and controller exited 0. The controller verified container and image retirement. [Hosted run](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44877259680)

The private ARM64 manifest changed only the size and SHA-256 of Torch's optional test shim. The existing AMD64 exclusion remains byte-identical. The guest verified the shim was absent and recorded exact identities for retained `libc10.so`, `libtorch.so`, and `libtorch_cpu.so`. Ten dependency modules imported from the relocated prefix with their expected origins. [ARM64 input inventory](rust-m10-native-arm64-python-host-inputs.md)

The guest did not import Torch or start Django, the Source application, or a plugin. This receipt covers standalone dependency assembly and the relocated module-origin check. Plugin execution, integrated ARM64 Compose and redistribution remain separate. Original Dispatcharr, CPython, PyTorch, and upstream authors retain full credit. [Machine-readable evidence](../scripts/rust-integration/m10-native-arm64-python-host-debian.json)
