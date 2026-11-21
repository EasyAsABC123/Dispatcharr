# Native ARM64 PostgreSQL client wrappers on Debian Bookworm

A native ARM64 GitHub runner built an isolated Debian Bookworm runtime
using the exact pinned donor closure. As UID 65532 in a read-only
container with networking disabled, it loaded the private library
paths and ran the version and help commands for `pg_dump`, `pg_restore`
and `psql`. Each reported PostgreSQL 17.11. The loader resolved 19, 19
and 20 private shared libraries respectively. The run verified all
546 copied files, including 20 original installed package notices and
475 public PostgreSQL support files. The build, wrapper and controller
exited zero, and the exact container and temporary image retired.
[Hosted wrapper proof](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44872005116)
[Donor input inventory](rust-m10-native-arm64-postgres-collector-inputs.md)

A prior local whitespace check exited 2 before push. The first hosted
build succeeded, followed by wrapper exit 2 and controller exit 1 with
exact cleanup. The successful retry made its bound fixture script
readable by the non-root process. The first artifact retained no raw
stderr or unresolved library marker.
[First hosted attempt](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44871563254)

The smoke covers standalone client loading and CLI execution. Native
database queries, dump and restore, the complete ARM64 application
package, corresponding source and distribution remain separate.
[Redacted receipt](../scripts/rust-platform-contracts/m10-native-arm64-postgres-debian-wrappers.json)

The original Dispatcharr, PostgreSQL, Ubuntu, Debian and dependency authors
and contributors retain full credit. [Attribution](../ATTRIBUTION.md)
