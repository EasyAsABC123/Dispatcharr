# Current AMD64 backup lifecycle component checks

Six isolated checks ran on the exact `2d3ffe53` development test binaries built offline with Rust 1.90. The four Store cases checked real SQL/cache ownership under backup callbacks. The two Job Transport cases exercised loss of the publication file descriptor and the logical deadline for status. Both retained the original caller until the actual reply. The independent audit verified exact cleanup for every case. [Redacted proof](../scripts/rust-integration/m8-2d3ffe53-amd64-backup-lifecycle-components.json)

Both Job Transport pools recorded size zero and residual idle count two. The Store tests preserved their capture-only mode. The complete same-owner Native backup workflow remains a prerequisite for the formal lifecycle row. [Current ARM64 backup failure and retry](rust-m8-2d3ffe53-arm64-backup-backend-loss.md)

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
