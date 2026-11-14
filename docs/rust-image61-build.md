# Image61 build checkpoint

Image61 builds committed revision `e96aac33399600df347c799e8ac5135d49f10d85`,
including the scoped opt-in DVR library. The actual Docker build exited 0 and
produced `sha256:9e463647fd7cb822b3b96822408b203f54029d7de7f2546d97f4ff1d83e6004c`.
All 1,370 archived application inputs match Git: 873 Rust, 495 Original React
and two credit inputs. The source-tree hash is
`907522536846b9c363a67ddec0e549096d71af0360a484e10ef003182cda2a48`.

A read-only probe exited 0 as UID 65532 and confirmed that all 114 installed
React/credit file hashes match image57. Two fresh exact cleanup queries were
empty. An earlier invocation stopped before Docker because its tooling PATH
was missing; its actual failure is preserved.

The [build packet](../scripts/rust-integration/image61-build/README.md) verifies
immutable Git inputs and reconstructs the exact normalized archive without
Docker or a mutable-checkout fallback. Original authorship and credits remain
intact. This is build and installed-file evidence. Main workflows, package/SDK
checks, full DVR acceptance and performance measurements need their own current
image executions. The [DVR library limits](rust-clocked-recording-current-library.md)
continue to apply, including process-exit retirement after post-claim uncertainty.
