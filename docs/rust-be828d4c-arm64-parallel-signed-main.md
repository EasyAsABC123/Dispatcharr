# BE82 three concurrent signed ARM64 fixtures

The exact ordinary native ARM64 BE82 package passed three simultaneous Main workflows. Fresh account login and token refresh, channel-group create/read/delete, and user-agent create/read/update/delete each ran in a separate Compose project. Each project used its own signing key, port, bridge and data volume. Every project rejected a sibling project's JWT with HTTP 401.

Transfer, cold verification, locked preflight, execution and independent readback each exited 0. All 12 fixture containers, three project networks and three data volumes retired. Full opening and closing Docker inventories matched on the same Linux boot.

Earlier attempts retain their distinct failures. The final run used the audited ARM64 platform image and config identities, the locally cached pinned ARM64 support images, and an absence check accepting the Docker CLI's exact `[]` error output only when the requested identity was reported missing. These are native functional results. The run measured no performance and grants no cross-platform or release-wide parity.

Original Dispatcharr and upstream contributors retain full authorship credit.
