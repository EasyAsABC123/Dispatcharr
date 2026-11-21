# AMD64 guarded PostgreSQL helper regression

The private guarded-helper draft based on `d608ff75` built the
`taskless-restore-helper` target on Linux AMD64. The build exited 0.
A fresh runtime probe used that exact image and exited 0 as UID 65532.
It used a read-only root filesystem, dropped all capabilities, and had
networking disabled. The probe copied the helper and its bundled libraries
to an executable tmpfs at their final `/opt` location.

The helper reported PostgreSQL 17.11. Its SHA-256 matched the build report
and image digest file. The probe verified x86-64 ELF, the system interpreter,
and the absolute DT_RPATH used by the packaged helper. The loader resolved
all six private libraries and reported no missing dependencies. The exact
probe container was verified absent after execution. [Build and runtime receipt](../scripts/rust-integration/m10-d608-amd64-guarded-helper.json)

Three earlier runtime probes on the same build each exited 1. The first
addressed the wrong staged executable name. The second ran outside the
helper's absolute library path and counted zero private resolutions. The
third used a tmpfs at the final path without the executable mount option.
Each probe container was independently verified absent. Their literal exits
and evidence hashes remain in the receipt.

Native ARM64 execution, the full AMD64 application build on this draft,
copied-data restore, and distribution qualification remain open.
[ARM64 builder inputs](rust-m10-arm64-builder-inputs.md)

The original Dispatcharr, PostgreSQL, Rust, Debian, and dependency authors
and contributors retain full credit. [Attribution](../ATTRIBUTION.md)
