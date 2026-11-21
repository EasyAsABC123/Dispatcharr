# Native ARM64 Python dependency-host inputs

The read-only native donor check identified notice inputs for all 127 installed Python distributions. The exact-version tracked tokenizers and tqdm notice files matched their hashes. It hashed all 90 Torch notice candidates and counted 12,566 Torch RECORD rows. [Hosted run](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44876783005)

The installed ARM64 Torch 2.14.1+cpu `torch/bin/test_shim` matched its wheel RECORD size and SHA-256. It is an AArch64 test executable of 688,016 bytes. The pinned upstream CMake source identifies the optional test target, and the current AMD64 exclusion has its own hash and size. This supplies a separate exclusion preimage for ARM64.

The probe exited 0 and verified exact container retirement. It checked input preimages and never imported third-party modules. Full dependency-host assembly, plugin execution, and redistribution remain open in this receipt. Original Dispatcharr, CPython, PyTorch and upstream contributors retain full credit. [Machine-readable evidence](../scripts/rust-integration/m10-native-arm64-python-host-inputs.json)
