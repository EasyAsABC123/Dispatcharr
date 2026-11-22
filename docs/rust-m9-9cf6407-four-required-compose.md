# Four M9 required checks on 9cf6407

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

Four required release-workflow rows passed fresh Docker Compose execution on ordinary Rust revision `9cf640701dacea07ea5f394ce7b18a6fa7c8c088`. Each row passed the independent workload and cleanup checks. The release coordinator exited 2 for its partial run. Four of 127 required rows ran and passed in this wave. The other 123 remain blocked in this receipt because their cases are outside this wave. The exact source, image, and proof hashes are in the [machine-readable evidence](../scripts/rust-integration/m9-9cf6407-four-required-compose.json).

| Required row | Result |
| --- | --- |
| Output Profile | Fresh Original and Rust evidence and exact cleanup passed |
| User-Agent | The single User-Agent child and its scoped verification passed |
| Complete Refresh Original | Fresh isolated Original reference run passed |
| Complete Refresh Rust | Rust replay matched the linked fresh reference under the same dispatcher lease |

Complete Refresh consumed the Original reference after 20.773 seconds, within its 900-second limit. The wave measured functional parity. Whole-service performance, soak, release rollback, and the remaining required rows still need their own release evidence.
