# Published copied SQLite React shell on native ARM64

Lappy's native Linux ARM64 Docker Desktop built the published Rust application
`a379cb70a325dec9de6f95ec97ad7e2a50e9602d` from the public migration branch.
The package used the previously qualified ARM64 runtime layers and rebuilt the
two Rust executables with the copied SQLite feature. Both executables ran as
UID 65532 with the bundled SQLite 3.46.0. The 2,142 application hashes preserve
all 497 React and credit inputs.

The Compose fixture generated fresh synthetic SQLite data on lappy from the
pinned Original source with the two previously recorded SQLite dialect
adapters. It kept the source database byte-identical and restarted Main after
a successful login and App Data write. The existing bearer read the saved
value after restart. Token refresh and deletion passed.

Both before and after restart, GET and HEAD succeeded for `/`, `/channels`,
and the referenced JavaScript and CSS. All four GET bodies matched the prior
Linux AMD64 package exactly. Each HTML response was 858 bytes. All five
owned containers and four live process generations retired, and the
project's container, network and volume inventories were empty.
[ARM64 receipt](../scripts/rust-integration/m10-a379-native-arm64-copied-sqlite-shell.json) ·
[AMD64 receipt](rust-m8-2c939204-copied-sqlite-react-shell.md)

Channel List JSON, full browser workflows, release-image qualification, and
performance remain open. The original Dispatcharr and dependency contributors
retain full credit. [Attribution](../ATTRIBUTION.md)
