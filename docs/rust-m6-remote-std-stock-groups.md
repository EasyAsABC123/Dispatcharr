# Original stock remote STD playlist group discovery

The original Dispatcharr authors and contributors retain full application
and playlist workflow credit. [Attribution](../ATTRIBUTION.md)

Pinned Original completed both stock remote playlist account workflows.
Both POST requests returned HTTP 201 and their Groups jobs completed
successfully. The inactive account made no origin request and remained idle
with empty group links. The active account made one origin request and
processed all 6,001 entries. It showed two enabled group links and a
pending-setup status. Both account-detail and group-list endpoints returned
HTTP 200 and agreed with the database. Neither workflow created streams.

The recorded SQL rows matched the observed group identifiers, enabled
settings and last-seen values. Both Original Compose roles and the UID
helper retired, and the host process groups settled.
[Validation receipt](../scripts/rust-integration/milestone-batch113/remote-std-stock-groups-original.json)

The current ordinary Rust POST requires a Core task broker before starting
the database change. The existing selected jobs mode binds that broker.
The opt-in Refresh All publisher serves its own route. Original publishes
Groups to the same default queue as its startup tasks, so the remaining
Native work is to qualify one owner for all required task families on that
queue and verify that its Groups writes appear through the stock endpoints.
[Selected worker scope](rust-m6-remote-std-create.md)

## Stock remote STD Groups with selected Rust workers

The exact `8b0c76ca` image passed the same active and inactive account
workflows in Compose against pinned Original. Rust ran with
`RUST_EXPERIMENTAL_SELECTED_JOBS=1` and `RUST_EXPERIMENTAL_SELECTED_BEAT=1`.

On both backends, each stock account POST returned HTTP 201 and produced
one Groups root that completed successfully. The inactive account made no
origin requests and had no group links. The active account made one origin GET, parsed
all 6,001 entries and produced a matching cache and two enabled group
links, Default Group and Owned Auto. Both stock admin GETs returned HTTP
200 and matched the account and group state in SQL, including last-seen
timestamps. Neither account created streams. The active and inactive
outcome projections matched exactly across the pair. Each backend's raw DB2
envelope lengths are recorded separately.

The earlier attempt exited 1 before the Original workflow because the
host could not reach the fixture origin's health endpoint on the internal
bridge. Native did not start. The outer cleanup flag remained false and
the independent cleanup receipt verified retirement of both observed
Original generations. The retry issued HTTP from inside the owned
namespace with no published ports. Both sides exited 0, their host process
groups settled, all five tracked service generations retired, and the
separate UID helper was removed.

Ordinary default-worker parity, full M6 acceptance and performance remain
open. [Redacted paired receipt](../scripts/rust-integration/milestone-batch113/remote-std-stock-groups-selected-8b0c.json)
