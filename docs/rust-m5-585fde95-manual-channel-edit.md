# Manual Channel and guide edits on 585fde95

The original Dispatcharr and React authors and contributors retain full credit. See [Attribution](../ATTRIBUTION.md).

Pinned Original and ordinary Rust AMD64 `585fde95` both returned HTTP 200 for the four requests in the manual Channel edit workflow. The fixture followed the existing React request sequence over HTTP in two separately owned Compose projects. The Rust arm used the dedicated selected programme receiver with Beat disabled. The Original arm retired Beat before the workflow. Browser DOM behavior has a separate gate.

| Request | Matched effect |
| --- | --- |
| GET XMLTV before the edit | The manual channel returned only `Owned programme A`. |
| POST Set EPG | The manual channel's guide ID moved from 5 to 6. Redis observed one root programme-parse task with guide 6, and the same task ID reached `SUCCESS` without child tasks. SQL gained the programme B row. |
| PATCH the channel | The channel name and stream order changed. The two stream-link row IDs, active profile membership, and guide ID were preserved. Redis observed no duplicate programme task. |
| GET XMLTV after the edit | The same channel returned only `Owned programme B`. |

The cache generation reached 2 after Set EPG and stayed there after PATCH. Both fixtures observed zero WebSocket events across the four request windows. The positive causal evidence comes from actual HTTP, Redis publication and task results, SQL, and XMLTV. The raw response fingerprints are retained per backend.

All owner and runtime exits were 0. Teardown confirmed both Compose projects and all five recorded container and process generations absent, with empty owned container, volume, and network inventories. The [machine-readable receipt](../scripts/rust-integration/m5-585fde95-manual-channel-edit.json) includes the isolated fixture IDs, per-arm effects, and exact image and teardown custody. Full M5 and performance acceptance have separate gates.
