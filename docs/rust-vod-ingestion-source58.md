# Original fresh Movie ingestion reference

Original Dispatcharr and dependency authors retain application, algorithm,
protocol and library credit. React and licensing remain unchanged.

The [Source58 packet](../scripts/rust-integration/vod-ingestion-source58/README.md)
records three real API cases: a non-XC account returns 400; VOD-disabled returns
400; an enabled XC account returns 202 and publishes the actual VOD root.
Both rejected cases leave SQL/cache and the queue unchanged. The positive root
reaches SUCCESS, creates two Movies, two provider relations and three category
links, leaves Series empty, and emits exactly two updates. Five real Requests
calls go through the existing owned TLS provider fixture. Inline Movie/Series/
cleanup helpers produce no queued children. Catalogue GET returns 200. Account
state, audit history and the declared Core cache scope remain unchanged.

The actual Source context exited 0 in 83.592 seconds with both EOFs and settled
process group. Independent cleanup verifies both fixture CIDs, both recorded
original positive host PIDs, project inventories and the UID probe container
absent. The UID host generation was not independently captured.

This characterizes a fresh basic Movie catalogue with empty Series and explicit
user agent, without metadata enrichment, language labels or updates. The Native
implementation is private pending final SDK/image/package and the complete
counterpart. General metadata, repeat/update ingestion, nonempty Series,
automatic M3U-to-VOD handoff and full M6 remain open. No performance result is
claimed by these functional timings or Source observations.
