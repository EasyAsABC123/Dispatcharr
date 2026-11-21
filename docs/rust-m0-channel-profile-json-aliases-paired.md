# Paired Channel Profile JSON reads

Pinned Original and the exact packaged Rust `d426d322b27a`
both passed 16 ordinary Channel Profile JSON alias GETs for list and detail
as an assigned Standard user and as an Administrator. Four canonical reads
in each arm supplied the controls. The aliases covered `?format=json`,
`.json`, `.json/`, and `.json?format=api` with JSON Accept.

All tested reads returned HTTP 200 and JSON. Within each arm every alias
matched its canonical response bytes. Across arms the normalized typed
profile records and enabled memberships matched. The assigned user saw
only the assigned profile. The Administrator also saw the peer profile.
The owned PostgreSQL profile, membership, assignment and channel rows
stayed unchanged through the reads.

The two Original and four Rust fixture roles retired, with their
process generations absent and empty owned Compose inventories. The
prior canonical and Stock-only receipts remain separate. This replay
qualifies this read cohort. [Redacted receipt](../scripts/rust-integration/m0-channel-profile-json-aliases-paired.json)

An earlier fixture preflight stopped at the application file count before
starting Compose. The same-image retry verified the full 2,036-file map.

The original Dispatcharr and dependency authors retain full credit.
[Attribution](../ATTRIBUTION.md)
