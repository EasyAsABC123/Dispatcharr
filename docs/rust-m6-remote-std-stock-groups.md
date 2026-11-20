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
