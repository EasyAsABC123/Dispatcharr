# Candidate8 recording playback and normal artwork

Original Dispatcharr and dependency authors retain full application, algorithm,
protocol and license credit. React remains unchanged.

Runtime `cb1ba69af8e0623222391a707a2b9fc03b5995f6` passes all four Rust workspace
gates: **915 tests passed, zero failed, 751 fixture-dependent tests ignored**.
All 1,189 inputs remain unchanged during execution, and every Rust input matches
Git. The [SDK evidence](../scripts/rust-integration/milestone-batch8/inventory.json)
retains the initial explicit-BigInt-type compilation failure and final pass.

The existing experimental recording mode now includes real streaming file/HLS
playback, Range/HEAD handling, playlist rewriting, viewer heartbeat, query/header
authentication, DVR/channel visibility and retained response ownership. Main
reuses its actual Auth/SQL/cache services. A combined dispatcher avoids the real
catchall/dynamic-route conflict and sends playback requests to their own Source
access policy before the general management/UI gate. Admission closes and actual
playback bodies drain before shared physical services close. The existing worker
flag controls these routes; no new configuration flag was added.

Normal artwork now handles genuine no-title/channel-title no-image branches,
manual EPG matching, channel-logo and existing VOD references, rating and season0
metadata while preserving Source's conditional-save behavior. A manual match
alone does not persist a programme unless another change requires saving. Legacy
constructors remain intact. External provider/image-fetch branches remain
unsupported; this is not complete artwork compatibility.

The [production image](../scripts/rust-integration/milestone-batch8-build/build.json)
is `sha256:b6ecb538995846c960066f1563e1ffc8ce407c7fd10b88df52eae7295a947c4d`.
Physical probes match all 1,681 archived inputs against Git: 1,184 Rust, 495 React
and two original credit/license files. All 114 earlier React/credit hashes remain
unchanged, with 120 selected asset/dependency-notice hashes verified. The actual
Rust entrypoint runs as UID65532 from `/app`; its ELF hash is
`9128a05481c56ae44c6b951acf494b652abb0360fdb98589ae0a7695304363c0`.
Separate GPG2.4.8/crypto/trust/license27-file checks and exact probe cleanup pass.
[Build commands and results](../scripts/rust-integration/milestone-batch8-build/inventory.json)
retain their actual exits.

The [affected selected-worker Compose cohort](../scripts/rust-milestone-batches/m478/batch8-functional/selected-m6/inventory.json) passes223 checks on this image,
including the configured/no-rules chains, startup roots, failure continuation,
future-head read bound and stricter actual data-only Native mounts. Full normal
recording controls, playback, recovery, Source task coverage and physical faults
still require the shared Source/Native recording cohort. Playback has explicit
32-reader/playlist/property bounds; larger and exceptional domains remain open.
No full migration milestone or paired performance gain is claimed.
