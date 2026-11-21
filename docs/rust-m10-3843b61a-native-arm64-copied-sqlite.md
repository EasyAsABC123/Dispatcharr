# ARM64 copied SQLite preview on Docker Desktop

The published `3843b61a` source built on lappy's Linux ARM64 Docker Desktop
daemon with the copied SQLite runtime enabled. Both ARM64 executables passed
non-root dynamic-link checks. Main links the bundled SQLite 3.46.0 amalgamation.
The private image retained the five layers of the qualified a8 runtime and
verified both executables and all three installed SQLite dependency notices.
The historical SQLite operator feature stayed off.

A fresh fixture migrated pinned Original source with two SQLite-only adapters.
The fixture checked 184 migrations, 63 tables, database integrity and foreign
keys, then copied the synthetic database byte for byte. The private Compose
project started the feature-on Main twice on that copy. The isolated client
logged in, read its profile and group collection, and saved App Data. It
observed the listener stop, read the saved value after restart with the same
bearer, refreshed the bearer, read its profile again and deleted the test data.
The synthetic master remained byte-identical.

The build and same-image restart both exited 0. Five exact container identities
retired, including both Main generations. All owned container, network and
volume inventories were empty. The project published no host ports and mounted
no Docker socket. The receipt preserves five earlier controller exits of 1.
Three stopped during build setup, one stopped before Compose at the donor
identity check, and one stopped at migration with an incomplete fixture
archive and retired its sole seed container. [Checked receipts](../scripts/rust-integration/m10-3843b61a-native-arm64-copied-sqlite.json)

The private React shell overlay, PostgreSQL task workflows, and distribution
remain outside this ARM preview. The original Dispatcharr, SQLite, SQLx and
dependency contributors retain full credit. [Attribution](../ATTRIBUTION.md)
