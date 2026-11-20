# Channel Batch physical write boundaries

Pinned Original and packaged Rust on revision 4faa831bcb6f each
accepted one ordinary 203-channel bulk PATCH. The selection crossed
the 100-row scalar and 200-row override implementation boundaries.
It verified 201 managed group overrides, including 198 existing
rows with unchanged clocks and pins and three new rows with clocks
inside the request window. Two manual rows changed their raw groups,
and the destination gained one custom provider relation.

Every response row matched the request order. Nine targeted GETs
matched the typed reads. Unselected SQL and links stayed unchanged.
The controller exited 0 and verified all four owned role generations
retired. Full M5 and performance remain open.

Original Dispatcharr contributors retain full application and behavior credit.
[Redacted receipt](../scripts/rust-integration/m5-channel-batch-boundaries-4faa831b.json)
