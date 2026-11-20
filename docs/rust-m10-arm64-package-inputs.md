# ARM64 package inputs for 4f9e3916

The offline audit matched the raw OCI indices below to the Dockerfile in
the exact 4f9e3916 source archive. Each index selects the listed Linux
ARM64 descriptor. The ARM64 child manifests, configs and layers were
absent from the retained local blob store.

| Build input | Pinned OCI index | ARM64 manifest descriptor |
| --- | --- | --- |
| Rust 1.90 Bookworm | `sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f` | `sha256:4c632e493dfa97f0fe014c3910d1690c149bba85ed8678d47d3563ec6f258ead` |
| Debian Bookworm slim | `sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc` | `sha256:ec8534cda2cb30f29f44f23e135297af7a1496f2990fd8e4e84aa36b081a757a` |
| Node 24 | `sha256:8fa96264f302f58bbb13aa7339c541547b56975766ce956c875da149452a3e40` | `sha256:0bab91d6a954a4a75de4a887edfda7386ed8711aa8b1eb5fb309ee0f0a2c544f` |

The guarded PostgreSQL helper's exact source archive contains five
AMD64 fallback packages. An ARM64 build still needs authorized native
Linux aarch64 execution, the pinned child image data and a trusted
Debian Bookworm ARM64 package inventory. The inventory must identify
the actual missing builder prerequisites via an authenticated package
index and bind each retained package's version, architecture and digest.
The native checks must verify the helper's ELF interpreter and private
library closure, non-root execution, embedded helper hash and an
ordinary copied restore.

The official Original ARM64 donor and its Comskip layer are verified
offline. The build used a moving Comskip source archive. Its retained
attestation and workflow records lack the exact consumed archive hash
or upstream revision. The existing donor still needs that source link
and its dependency notices. A separately pinned rebuild would need
corresponding source, its own notices and native functional evidence.
[Official donor evidence](../scripts/rust-integration/milestone-batch111/arm64-original-donor.json) ·
[Comskip build provenance](../scripts/rust-integration/milestone-batch111/arm64-comskip-provenance.json)

Native ARM64 package, workload and distribution qualification remain
open. No containers ran during this audit.

Original Dispatcharr, PostgreSQL, Comskip, FFmpeg and dependency authors retain full credit.
