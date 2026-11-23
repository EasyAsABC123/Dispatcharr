# M10 ARM64 common-ingress rollback rehearsal

The exact `0c5133b62dc9f2df0900f2228e25b75baecb6754` ARM64 package
`sha256:8d26782ba4a364efc735e55c86e7c4790f38e4ab339727158f3d3a9ab02c95a9`
ran in a disposable Compose project on Lappy. Original was
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The rehearsal used a private
network and published zero host ports.

One internal ingress process served the same signed session through
**Original → Rust → Original**. Its exact 17-request ledger verified the
target generation for each request and drained active requests before
both route changes. Rust acknowledged one channel rename against the
copied database. After its writer retired, warm Original reconciled the
write and read the same row through the same ingress and signed session.
The changed fields were `name` and `updated_at`. Schema, sequences,
and unrelated rows matched, and four full warm Source observations retained
the Original web, PostgreSQL and Redis generations. An independent Original
startup control qualified the four expected startup stamps.

The preflight, physical runner, independent proof and outer owner each
exited 0. Exact cleanup verified retirement of all 17 recorded resources
and empty project inventories for containers, networks, and volumes.
The result covers an isolated one-write warm rollback with a common
front door. Strict cold equivalence, the full ARM64 release workflow and
M10 controlled rollout remain open.

The original Dispatcharr authors and contributors retain full credit
for Dispatcharr and its existing React frontend. See the
[attribution](../ATTRIBUTION.md) and [license](../LICENSE).

[Machine-readable evidence](../scripts/rust-integration/m10-0c5133b-arm64-common-ingress.json)
