# Native ARM64 PostgreSQL client roundtrip

The proven Debian Bookworm ARM64 client wrappers connected to PostgreSQL
17.11 from the pinned Original Dispatcharr donor over an internal
two-container network. The server independently verified the fixture-only
SCRAM rule. The wrappers queried three generated rows, wrote a 1,872-byte
gzip-compressed custom archive and restored it in a single transaction
into a separate fresh database. Both databases returned the same rows,
primary key and check constraint types.
[Hosted roundtrip](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44873669389)
[Standalone wrapper proof](rust-m10-native-arm64-postgres-debian-wrappers.md)

The build, client, server and controller exited zero. Both exact
containers, their internal network and the temporary client image
retired. The fixture used generated data and retained no credential
values. Full ARM64 application packaging and distribution remain open.
[Redacted receipt](../scripts/rust-platform-contracts/m10-native-arm64-postgres-client-roundtrip.json)

The first two hosted attempts each built successfully, then recorded
client and controller exit 1 with exact cleanup. The second reported an
authentication error category on the first version query. The successful
sibling added and checked one scoped SCRAM rule for the internal network,
fixture role and its three database names.
[First attempt](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44872667384)
[Diagnostic attempt](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44873145017)

The original Dispatcharr, PostgreSQL, Ubuntu, Debian and dependency
authors and contributors retain full credit. [Attribution](../ATTRIBUTION.md)
