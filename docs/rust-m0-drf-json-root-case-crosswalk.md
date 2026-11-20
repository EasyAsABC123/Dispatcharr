# DRF directory JSON case crosswalk

The published Original and packaged Rust pair matched 64 ordinary
`GET` requests with `Accept: application/json` on 16 literal paths.
Each path returned 401 with an authentication challenge for Anonymous,
403 for Viewer and Standard, and 200 for Administrator. The paired
receipt records matching JSON media type, body length and SHA-256 for
each case. Both arms preserved the owned actor rows. The independent
cleanup check verified retirement of all four owned role generations
across the pair.

The Source operation and paired case columns are 1-based positions in
the respective receipt arrays `operations` and `ordinary_JSON_cases`.
Each Source operation carries its four role observations in table order.
The work IDs and existing Source attachments come from the canonical
register. [Original receipt](../scripts/rust-integration/milestone-batch111/drf-api-roots-original.json) ·
[Paired receipt](../scripts/rust-contracts/verified-native-drf-api-roots-8a821e27.json) ·
[Canonical register](rust-contract-register.json)

| Literal GET path | Existing Source contract work ID | Source operation | Anonymous 401 | Viewer 403 | Standard 403 | Admin 200 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `/api/accounts/.json` | `route-3972b5c042ea0642f404:METHOD_UNRESOLVED` | 1 | 1 | 2 | 3 | 4 |
| `/api/accounts/` | `route-2f0ebbed429f07494c01:METHOD_UNRESOLVED` | 2 | 5 | 6 | 7 | 8 |
| `/api/hdhr/.json` | `route-57610369344cb452225c:METHOD_UNRESOLVED` | 3 | 9 | 10 | 11 | 12 |
| `/api/channels/.json` | `route-482aac7e9cad73e20e87:METHOD_UNRESOLVED` | 4 | 13 | 14 | 15 | 16 |
| `/api/channels/` | `route-6b777e58c3150f35aa9c:METHOD_UNRESOLVED` | 5 | 17 | 18 | 19 | 20 |
| `/api/connect/.json` | `route-057f3d60fbf41f71fa3b:METHOD_UNRESOLVED` | 6 | 21 | 22 | 23 | 24 |
| `/api/connect/` | `route-0b9461af78ded81781c0:METHOD_UNRESOLVED` | 7 | 25 | 26 | 27 | 28 |
| `/api/core/.json` | `route-d87402d3d74780c4b662:METHOD_UNRESOLVED` | 8 | 29 | 30 | 31 | 32 |
| `/api/core/` | `route-5a09390ffbfb3917778e:METHOD_UNRESOLVED` | 9 | 33 | 34 | 35 | 36 |
| `/api/epg/.json` | `route-8062d0712308caa75291:METHOD_UNRESOLVED` | 10 | 37 | 38 | 39 | 40 |
| `/api/epg/` | `route-42bf7668924bcaacce2c:METHOD_UNRESOLVED` | 11 | 41 | 42 | 43 | 44 |
| `/api/m3u/.json` | `route-0ef353e279c00a9bea3c:METHOD_UNRESOLVED` | 12 | 45 | 46 | 47 | 48 |
| `/api/m3u/` | `route-6ef4eb2b70aebf6471ee:METHOD_UNRESOLVED` | 13 | 49 | 50 | 51 | 52 |
| `/hdhr/.json` | `route-dc4da9ca250036ec34bc:METHOD_UNRESOLVED` | 14 | 53 | 54 | 55 | 56 |
| `/api/vod/.json` | `route-9a06cadab4a398087cb0:METHOD_UNRESOLVED` | 15 | 57 | 58 | 59 | 60 |
| `/api/vod/` | `route-071cc4ed86bb92480c20:METHOD_UNRESOLVED` | 16 | 61 | 62 | 63 | 64 |

- Original revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`, image
  `sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f`, published Source receipt
  SHA-256 `7218a26d718b33ca72db7c9529267fcb5bc4febc4feb9a5a0cd71ecaddfea385`. The paired receipt pins the
  retained physical Source receipt SHA-256 `585b0f741b6ccc5869ec686e789bb77b9048bdd2a82d7e1b0acba32e4e28d68c`.
- Packaged Rust revision `8a821e27adb01df52898e971661a20a9362c54a1`, image
  `sha256:1072f1ef8189ef545fef962777183c1ab13cb2e64fea3c15f279c496520eca46`, build manifest
  SHA-256 `7e2f6dc9464c85de26543a434f7051b558b213d11a04fe300c0f3c90288379d9`, published paired receipt
  SHA-256 `bc6ea77dd071c7bf0916f5794698ce414715aaa43d0aadd61d7c53f30848345b` and physical pair receipt
  SHA-256 `1e6b361e7e8211aa68efa5be19b36a1e239335ab392277baf237d844c8ca8ceb`.
- Register snapshot SHA-256 `ca7074f08ecb7ba19439ce434cc20d2ca3b97249f5c2bb159bfc435863a93f06`.

GET is an observed method for these attached cases. The canonical
resolver method remains unresolved in all 16 work IDs. Other methods,
other negotiation inputs and requests to linked resources remain open.
Native browsable HTML remains pending in this 8a821e27 comparison.
The crosswalk adds evidence pointers and preserves the existing
register counts and verification states.

Original Dispatcharr and Django REST Framework contributors retain full
credit. [Dispatcharr upstream](https://github.com/Dispatcharr/Dispatcharr)
