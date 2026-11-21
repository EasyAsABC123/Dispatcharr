# Group Manager wire evidence on 2025258

The original Dispatcharr authors and contributors retain full application and
React credit. [Attribution](../ATTRIBUTION.md)

## Group claim and rename response differences

A fresh paired Compose run against packaged Rust
`2025258fba9aa301a4a87b4ea12391087b5c55a7` repeated the eight ordinary Group
Manager requests. All eight matched Original's `Allow` header and passed the
typed business and SQL assertions. It reproduced the same five response-body
digest differences as the previous run.

| Response | Observed difference |
| --- | --- |
| Rename | Newly allocated renamed-group ID at `/id` |
| Renamed-group detail | Newly allocated renamed-group ID at `/id` |
| Manual channel detail | Renamed-group references at `/channel_group_id` and `/effective_channel_group_id`, and top-level JSON key order |
| Managed channel detail | Top-level JSON key order |
| Group collection | Newly allocated renamed-group ID at `/1/id` |

The typed values matched in all eight responses after normalizing that
fixture-allocated group reference. The comparison checked key presence, types,
number representation, array order, and field values. The receipt contains
paths and categories. Raw response bodies, credentials, and temporary
comparison values stayed out of the retained evidence. The paired controller
exited 0 and all four owned Compose roles retired.
[Paired diagnostic receipt](../scripts/rust-integration/m5-2025258-group-manager-field-diagnostic.json)

## Stock authenticated OPTIONS

The pinned Original at `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` returned
HTTP 200 and the same ordered six-field group schema wherever it declared an
action. The provider relation nested ten declared fields.

| Route | Allow | Action in metadata | Compact JSON bytes |
| --- | --- | --- | --- |
| Group collection | `GET, POST, HEAD, OPTIONS` | POST | 1939 |
| Existing group detail | `GET, PUT, PATCH, DELETE, HEAD, OPTIONS` | PUT | 1942 |
| Existing group's claim-custom | `POST, OPTIONS` | POST | 1993 |
| Unallocated group detail | `GET, PUT, PATCH, DELETE, HEAD, OPTIONS` | Omitted | 181 |
| Unallocated group's claim-custom | `POST, OPTIONS` | POST | 1993 |

Each response used `application/json` and `Vary: Accept, origin`. The
fixture sent OPTIONS exclusively, observed unchanged snapshots across all
eight scoped SQL tables, exited 0, and retired both owned Compose roles.
These results establish the Original metadata contract for the tested
authenticated JSON requests. Full M5 acceptance and comparative performance
remain open.
[Original OPTIONS receipt](../scripts/rust-integration/m5-2025258-group-manager-stock-options.json)
