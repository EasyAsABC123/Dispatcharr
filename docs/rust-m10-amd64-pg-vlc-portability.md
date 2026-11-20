# amd64 PostgreSQL and VLC package portability

Original Dispatcharr, PostgreSQL, VideoLAN, and dependency authors retain full
credit for their application and tools.

The exact amd64 application and standalone backup-component images built
successfully. Fifteen runtime probes then passed against those retained
images with networking disabled. Both packages exposed the same PostgreSQL
17 payload sources and hashes and ran pg_dump, pg_restore, and psql version
checks. The application also passed the packaged VLC and cvlc checks.
The controller verified the selected image identities and retired all
fifteen probe containers.

The first runtime attempt used a platform manifest digest where the local
engine expected the retained image tag. A second attempt reached the VLC
wrapper and selected the wrong version-banner expectation. The final replay
kept both built images and used the observed pinned VLC banner.

This qualifies the packaged amd64 PostgreSQL and VLC tool boundary. The full
application release matrix remains open.

[Exact portability receipt](../scripts/rust-integration/milestone-batch111/amd64-pg-vlc-portability.json)

## a785 amd64 package check

Ten probes passed on the exact `a785a2b7` application image. Five comparison
probes passed on the retained standalone backup-component image. Both exposed
the same PostgreSQL 17 payload sources and hashes. The `pg_dump`, `pg_restore`
and `psql` version commands matched their packaged manifests. The application
also passed the packaged VLC and cvlc wrapper and version checks.

The probes used the retained images with networking disabled. The controller
verified the amd64 image identities and independently confirmed all 15 probe
containers were removed. Rollout and native ARM64 package validation remain
open.

[Exact a785 amd64 receipt](../scripts/rust-integration/milestone-batch111/a785-amd64-pg-vlc-portability.json)

## ARM64 donor provenance

The published `base-20261103010601` index and its ARM64 tag index resolve to
`linux/arm64` manifest
`sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace`.
The manifest binds config
`sha256:aa357ada16661964a9af41fe289964ae8008460b60efcdbbad60c4383e7d3b43`,
whose image labels identify Original revision
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The same release index contains
the existing amd64 donor. [Official Dispatcharr package versions](https://github.com/orgs/Dispatcharr/packages/container/dispatcharr/versions?filters%5Bversion_type%5D=tagged)

The declared Comskip layer passed both its compressed SHA-256 and the
config's uncompressed layer digest check before inspection. It contains a
332,800-byte AArch64 ELF at `/usr/local/bin/comskip` with interpreter
`/lib/ld-linux-aarch64.so.1` and SHA-256
`75bc4daf372684c366fbd86a3d53106f12d1be571a63ed4d55bc28d400ab4692`.
Both platform preflights passed offline. The validation host was x86-64,
and no ARM64 code ran. [Exact ARM64 donor receipt](../scripts/rust-integration/milestone-batch111/arm64-original-donor.json)

On a native Linux ARM64 worker, the next check is to run this exact detector
with networking disabled on the retained synthetic H.264/AAC recording and
INI. Require exit 0 and complete loader resolution, then compare the generated
EDL with the retained Original amd64 digest
`984238c523502b3c23dc3e07e9fcdaf3bead2446328e74315bec4671af4dfbbc`
for that exact input. Package qualification also requires the app and
standalone backup builds, native PostgreSQL dump and restore, FFmpeg and VLC
playback, the packaged Comskip wrapper, GPG and Python/regex compatibility
checks, stock frontend/DRF checks and exact cleanup. The existing amd64
PostgreSQL/VLC receipt remains scoped to its two tested images.
[Synthetic Comskip inputs](../scripts/rust-integration/comskip-positive-assets/README.md)
[Original amd64 Comskip result](../scripts/rust-integration/comskip-positive-source1/positive-mark/actual-generated-EDL.json)

Original Dispatcharr and upstream Comskip authors retain all credit. The
current Comskip notice records the amd64 binary and the Original recipe
fetched a moving upstream `master`. The ARM64 binary identity leaves its
corresponding source and dependency notices unresolved for distribution.
[Original build recipe](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/docker/DispatcharrBase)
[Retained Comskip notice](../rust/runtime/comskip-NOTICE)

### Comskip build attestation

The official ARM64 index points to attestation manifest
`sha256:101a3215d5d658cb5f55b23333cc784e79849e39981111417d0f4e4e3a18aed9`
and SLSA v1 statement
`sha256:ccaee1b02aa0f09dabfd5e41fb1270655611aa110dc9018e1d4dd8c38fee17ba`.
Their digests and subject match the exact Original image. The statement's
embedded recipe matches the pinned Dockerfile. BuildKit ties its Comskip copy
step to the verified ELF layer. The recorded build command downloads a moving
`master` archive. The statement marks its resolved-dependency list incomplete
and lists only the two ARM64 base images.
[Official ARM64 attestation descriptor](https://ghcr.io/v2/dispatcharr/dispatcharr/manifests/sha256:101a3215d5d658cb5f55b23333cc784e79849e39981111417d0f4e4e3a18aed9)

The authenticated workflow artifacts `13194869636` (ARM64) and `13196150368`
(amd64) matched their declared sizes and OCI digests. Their decoded records
each identify one uncached Comskip build step. The retained ARM64 step output
covers 681 chunks and 99,115 bytes. The official attestation and build
records did not identify the fetched upstream commit or archive digest. An
authoritative revision or exact archived source remains needed to establish
corresponding source for this binary. Native ARM64 runtime and redistribution
qualification remain pending. The Original Dispatcharr and Comskip authors
retain full credit.
[Original build run](https://github.com/Dispatcharr/Dispatcharr/actions/runs/41557436058/attempts/1)
[Exact attestation and build-record receipt](../scripts/rust-integration/milestone-batch111/arm64-comskip-provenance.json)
