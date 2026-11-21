# Native ARM64 VLC donor inventory

A native `ubuntu-24.04-arm` runner inventoried the pinned Original
Dispatcharr ARM64 donor. The launcher verified its OCI manifest, config,
and 17 layer diff IDs before running the read-only, network-disabled
probe as UID and GID 65532. The probe and controller each exited 0.
The launcher confirmed exact container removal and an empty owned
container inventory. [GitHub Actions run 44871964753](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44871964753)

| Observed inventory | Count |
| --- | ---: |
| Native plugin ELF files | 339 |
| Generated plugin caches, outside collector intake | 1 |
| ELF roots with loader checks, including VLC | 340 |
| Resolved libraries, including four NSS companions | 201 |
| Public VLC data files | 63 |
| Package notice records | 185 |

All tested loader roots resolved. The notice inventory had one gap.
The exact path `/lib/ld-linux-aarch64.so.1` had a `diversion-from`
annotation in the package inventory, so the probe did not attribute an
owning-package notice to those bytes. The receipt keeps
`license_inventory_complete=false`.

The first two attempts exited 1 in the probe and verified exact cleanup.
The first retained no underlying exception. The second pinpointed a
package-owner parser assertion for a matching path. The third exited 1
in host preflight because its branch guards disagreed. It created no
container, and the generic cleanup field stayed false. [First run](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44870688001), [Second run](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44871054421), [Third run](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44871562974)

The successful run covers donor inventory. ARM64 Rust package intake,
VLC playback, and release publication remain open.
The [machine-readable receipt](../scripts/rust-integration/m10-native-arm64-vlc-donor.json)
records the validation revision and GitHub artifact checksums.

Original Dispatcharr, VideoLAN, Debian/Ubuntu and dependency authors and
contributors retain full credit.
