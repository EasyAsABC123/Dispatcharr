# Native ARM64 Original donor library inventory

The native ARM64 job inventoried the exact Original media donor on Ubuntu 26.04 Resolute. The image matched the pinned OCI manifest and config and all 17 layers. A non-root, read-only container with no network checked six installed ARM64 library files, the AArch64 interpreter, their package ownership, and installed notices. The guest and controller exited 0, and the exact container and its labeled inventory were absent after cleanup. [Successful donor job](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44864240090/job/136724220373) · [Redacted receipt](../scripts/rust-integration/m10-native-arm64-comskip-donor-inventory.json)

| Installed source | Exact version | Direct library ABI |
| --- | --- | --- |
| FFmpeg | `7:8.0.1-3ubuntu2` | `libavcodec.so.62`, `libavformat.so.62`, `libavutil.so.60`, `libswscale.so.9` |
| Argtable2 | `13-4build1` | `libargtable2.so.0` |
| glibc | `2.43-2ubuntu2.4` | `libc.so.6` and the native interpreter |

The first hosted attempt pulled the exact image and exited 125 when the Docker CLI rejected `image inspect --platform`. The retry used compatible image inspection and verified the pinned repository digest, config ID, architecture and layer identities before creating the container. [First hosted job](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44863915272/job/136723227023)

Official GHCR returned HTTP 200 for anonymous Original access and 401 for the separate attested FFmpeg builder. Raw manifest and config bytes for that builder remain pending. The exact donor package versions can guide signed header selection for a future build of the pinned Comskip replacement. This run did not execute either detector. Target-ABI compilation, the replacement's transitive distribution closure, and full ARM64 application packaging remain open. [Original attestation](../scripts/rust-integration/milestone-batch111/arm64-comskip-provenance.json) · [Native replacement smoke test](rust-m10-native-arm64-comskip-candidate.md)

The original Dispatcharr authors, Erik Kaashoek and Comskip contributors, and FFmpeg, Ubuntu and dependency authors retain full credit. [Attribution](../ATTRIBUTION.md)
