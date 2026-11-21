# Native ARM64 CLI and Compose on 9116a8cc

A native ARM64 runner built the exact published application `9116a8cc54cebe988ab939de70e4f761d45a3db7` with the checked-in CLI and verified the nine component exports, UID 65532, and Original license notices. The same retained image passed all 12 ordinary Compose checks, including clean install, authenticated restart, and two concurrent readers with one active MPEG-TS upstream. Both readers decoded audio and video. [Verified native workflow](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44919393171)

A separate copied PostgreSQL fixture on that image restored 62 tables and 184 migration records. A normal account login and channel read passed after restore, with exact retirement of the image, projects, and test files. The exact application also has a verified AMD64 build. Distribution, current default-backend acceptance, and comparative performance remain open. [Redacted platform receipt](../scripts/rust-integration/m10-9116a8cc-native-arm64-cli-compose.json)

Original Dispatcharr authors and all dependency contributors retain full credit. [Attribution](../ATTRIBUTION.md)
