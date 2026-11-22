# Numeric-user AppData reads and writes on f960875

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

Two fresh Original-to-Rust Compose comparisons exercised numeric-user AppData. Owner and Admin read both the collection and a stored key. An unrelated Standard user received 403 on both routes. The read family left scoped SQL unchanged. On the key route, successful Owner and Admin PUTs persisted and read back their values. Their DELETEs returned 204. The unrelated Standard user's valid PUT and DELETE each returned 403 and left the rows unchanged. The fixture restored its one owned mutable row and verified unrelated state.

All selected response bodies matched literally and both owners and independent verifiers exited 0 with exact teardown. Wire equality remains open. Both families showed Allow and Vary differences. The two successful DELETE cases also differed in Content-Length. The machine-readable observations retain the differences and keep the two route IDs separate.

This is scoped semantic and database evidence. The formal register and the broader M0 and M5 gates remain open. [Machine-readable evidence](../scripts/rust-integration/m0-m5-f960875-numeric-app-data.json)
