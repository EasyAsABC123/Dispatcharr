# Cached XC information on the ordinary package

Pinned Original Dispatcharr and the ordinary Rust package built from
`55c895bfe5637541788fe0445cd968c5f8f52ffc` each served eight authenticated
GETs for current cached XC VOD and Series information. The administrator,
standard and basic accounts and the account configured to hide adult content
each received HTTP 200, application/json, and identical response bytes on
both backends. The fixture used the running wall clock.

The SQL data and sequences remained unchanged for every request. The owned
provider origin recorded zero requests. The paired controller exited 0,
all four owned container generations retired, and the container, volume and
network inventories were empty.
[Redacted receipt](../scripts/rust-integration/m7-55c895-cached-xc-info.json)

This result covers fresh cached information reads on the exact ordinary
image. Stale and uncached refresh, other XC actions, copied SQLite and
performance require separate evidence. Original Dispatcharr and dependency
authors retain full credit.
[Attribution](../ATTRIBUTION.md)
