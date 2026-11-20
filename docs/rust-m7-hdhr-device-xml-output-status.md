# HDHomeRun device XML and output-profile Lineup Status

Original Dispatcharr authors and contributors retain all application credit.

A local Compose run compared pinned Original `1241b3f7` with packaged Rust
`e852e760` using six GET requests per backend.

- `/hdhr/device.xml` and `/api/hdhr/device.xml`
- `/hdhr/output_profile/{id}/lineup_status.json`
- `/hdhr/HDHR%20fixture/output_profile/{id}/lineup_status.json`

The status requests used the same active and inactive output profiles on each
backend. Every response returned HTTP 200 and matched the complete body digest
and selected headers of the corresponding live Original response. XML was
542 bytes per alias and JSON was 84 bytes per status request. Selected headers
included Content-Type, Content-Length, Allow, Vary, Location and
WWW-Authenticate. Both apps received the same fixture Host header. The retained
historical XML authority projects that fixture host into the two URL fields.

The scoped SQL snapshots stayed unchanged on both backends. Compose teardown
retired the Original app, media fixture, data-init role and Rust app. The
project retained zero containers, networks and volumes, and the runner exited 0.

This extends the separate [Discovery and Lineup](../scripts/rust-integration/milestone-batch113/hdhr-discovery-lineup-exact-wire.json)
and [ordinary Lineup Status](../scripts/rust-integration/milestone-batch113/hdhr-lineup-status-f3b8.json)
results. Full M7 acceptance and performance remain open.

[Redacted scoped receipt](../scripts/rust-integration/milestone-batch113/hdhr-device-xml-output-status-e852.json)

## Integrated a785 metadata group

One internal Compose comparison ran 27 ordinary positive GETs per backend
against pinned Original and packaged Rust a785a2b7. It combined the 14
discovery and lineup requests, seven ordinary Lineup Status requests and
six XML and output-profile status requests. All 54 requests returned
HTTP 200. Every pair matched body lengths, full body digests and selected
wire headers. The scoped SQL snapshots remained unchanged throughout each
subgroup. The manual-backup owner was absent from Rust Main.

The controller exited 0. All four container and host generations retired
and the project inventories were empty. The earlier f3b8 and e852 receipts
remain tied to their respective images.

[Redacted a785 grouped receipt](../scripts/rust-integration/m7-hdhr-ordinary-group-a785.json)
