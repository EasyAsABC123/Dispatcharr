# Original administrator channel reorder

The original Dispatcharr and Django authors retain full credit.

Pinned Original passed five authenticated reorder requests and six ordered channel-list reads in an isolated Compose fixture. The actions covered a manual channel moved down, a managed channel moved up, a managed channel moved to the start, a repeated move to the same position, and a managed channel returned to its provider number. Every request returned HTTP 200. The action message and the raw and effective numbers in each response matched the database and the order displayed by the subsequent list request.

The fixture verified the affected channel numbers and override pins, including a fractional number that shifted by one. A reorder shifted the numbers of the ten seeded channels across groups when they fell within the affected range. Managed channel numbers stayed pinned separately from their provider numbers where applicable. A new number-only override had both Django timestamps populated within its POST. Returning the channel to its provider number removed that override. The repeated move and all list requests left SQL unchanged.

The first run exited 1 when its fixture expected the two required override timestamps to be null. That run passed cleanup. The corrected run exited 0 with both Compose containers and host process generations absent and empty project inventories. Rust and full M5 acceptance remain open.

[Redacted receipt](../scripts/rust-integration/milestone-batch113/original-channel-reorder.json)
