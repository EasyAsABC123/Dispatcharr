# Native ARM64 Compose acceptance on 21da

This run rebuilt the exact Rust application `21da6d0a7cbe0cd5cb8b757881b89e36f755be9f` on GitHub's native ARM64 runner. The protected React, license, and attribution files matched the earlier 0f79 run. That run provided the controller procedure. This result comes from the fresh 21da package. [Machine-readable evidence](../scripts/rust-integration/m10-native-arm64-21da-clean-install-playback.json) · [Earlier 0f79 run](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44892036200)

The hosted runner built all nine components and verified hashes, sizes, and timestamps for 27 pinned DRF files as UID 65532. All 12 Compose checks passed through the runner's internal bridge without a published host port. The checks included empty PostgreSQL, the initialization-off control, the React index, first-admin setup, login, and a restart preserving the migration records.

Two authenticated viewers opened the same MPEG-TS channel. Each delivered 197,400 packet-aligned bytes that decoded to audio and video. One upstream connection served both viewers, then closed with one lifetime connection recorded. The runner verified retirement of all owned test Compose resources and the private image. [Exact hosted run](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44893810107/job/136815075183)

The receipt covers the private native package and the ordinary Compose checks recorded here. Browser testing, comparative performance, and image distribution remain outside this scope. Full credit belongs to the original Dispatcharr authors and dependency contributors. [Original attribution](../ATTRIBUTION.md)
