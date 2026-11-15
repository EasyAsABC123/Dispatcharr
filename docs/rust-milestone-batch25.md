# Shared Redirect assignments and plugin runtime engineering

Original Dispatcharr and dependency authors retain full application, media,
algorithm and licensing credit. The React frontend remains unchanged.

This candidate resolves Redirect requests from the actual Source channel/stream
aliases, distinguishes borrowed assignments from owned reservations, and uses
current database credentials without allocating another provider slot for a
borrowed assignment. Ordinary native proxy owners publish those aliases from
their existing live lease. Failover and retirement use the existing ownership
ledger; VOD and preview identities cannot masquerade as ordinary channels.
The existing raw owner, counter and connection budgets remain authoritative.
Foreign Source aliases have no epoch; stronger ABA guarantees are not claimed.
Inverse Redirect-to-proxy and wider preview behavior remain open.

The same batch compiles concrete plugin runtime infrastructure: lazy owned
interpreter actors, same-session SQL compatibility, cache/events ports, loaded
callable identity, bounded shared results, causal writer ownership and real
nonextending shutdown deadlines. It does not activate enabled plugins in Main.
The generic worker, actions, host packaging and transaction/child-process
acceptance remain active engineering work. A module registration is not a
working enabled-plugin deployment.

All [four SDK gates](../scripts/rust-integration/milestone-batch25/inventory.json)
pass: formatting, workspace compilation, strict Clippy and workspace tests.
The batch binds1,350 inputs (1,344 Rust) and43 changed paths. There are1,061
passing tests, zero failures and775 ignored tests across177 result blocks.
Seven new Redirect Redis controls are prepared but remain unexecuted. Earlier
attempts exposed Result return typing, a missing owned deadline method and
strict lint issues; their failures remain recorded before the passing revision.

Docker packaging, actual Source/Native Redirect and corrected affected workflow
acceptance, enabled-plugin physical proof and paired resource measurement remain
required. Historical Candidate24 results do not accept this candidate. No full
M0–M9 milestone or new application performance gain is accepted.
