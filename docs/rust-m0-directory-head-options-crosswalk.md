# API directory HEAD and OPTIONS case crosswalk

The 6e83 comparisons attach two ordinary administrator cases to each of the
same 16 directory work IDs used in the earlier GET comparison. All 16 HEAD
cases matched at the shipped Original nginx boundary, including the empty
entity. The direct uWSGI comparison had literal exit 1 and retains its own
receipt. All 16 OPTIONS cases matched the ordered JSON metadata, headers,
and full payload digest on the two backends.

The HEAD position refers to the independently compared stock and Rust wire
observations. The OPTIONS position refers to the paired receipt array
`same_ordered_intrinsic_wire_per_path`. Positions are 1-based. The canonical
resolver entries retain their open method discovery and full verification
states.

| Literal path | Existing contract work ID | HEAD position | OPTIONS position |
| --- | --- | ---: | ---: |
| `/api/accounts/` | `route-2f0ebbed429f07494c01:METHOD_UNRESOLVED` | 1 | 1 |
| `/api/accounts/.json` | `route-3972b5c042ea0642f404:METHOD_UNRESOLVED` | 2 | 2 |
| `/api/channels/` | `route-6b777e58c3150f35aa9c:METHOD_UNRESOLVED` | 3 | 3 |
| `/api/channels/.json` | `route-482aac7e9cad73e20e87:METHOD_UNRESOLVED` | 4 | 4 |
| `/api/epg/` | `route-42bf7668924bcaacce2c:METHOD_UNRESOLVED` | 9 | 9 |
| `/api/epg/.json` | `route-8062d0712308caa75291:METHOD_UNRESOLVED` | 10 | 10 |
| `/api/hdhr/.json` | `route-57610369344cb452225c:METHOD_UNRESOLVED` | 11 | 11 |
| `/api/m3u/` | `route-6ef4eb2b70aebf6471ee:METHOD_UNRESOLVED` | 12 | 12 |
| `/api/m3u/.json` | `route-0ef353e279c00a9bea3c:METHOD_UNRESOLVED` | 13 | 13 |
| `/api/core/` | `route-5a09390ffbfb3917778e:METHOD_UNRESOLVED` | 7 | 7 |
| `/api/core/.json` | `route-d87402d3d74780c4b662:METHOD_UNRESOLVED` | 8 | 8 |
| `/api/vod/` | `route-071cc4ed86bb92480c20:METHOD_UNRESOLVED` | 14 | 14 |
| `/api/vod/.json` | `route-9a06cadab4a398087cb0:METHOD_UNRESOLVED` | 15 | 15 |
| `/api/connect/` | `route-0b9461af78ded81781c0:METHOD_UNRESOLVED` | 5 | 5 |
| `/api/connect/.json` | `route-057f3d60fbf41f71fa3b:METHOD_UNRESOLVED` | 6 | 6 |
| `/hdhr/.json` | `route-dc4da9ca250036ec34bc:METHOD_UNRESOLVED` | 16 | 16 |

Original Dispatcharr and Django REST Framework contributors retain full credit.
[HEAD receipt](../scripts/rust-integration/m0-stock-nginx-directory-head-6e83d3d.json) · [OPTIONS receipt](../scripts/rust-integration/m0-native-directory-options-6e83d3dba839.json) ·
[GET crosswalk](rust-m0-drf-json-root-case-crosswalk.md) · [Machine-readable crosswalk](../scripts/rust-integration/m0-directory-head-options-crosswalk.json)
