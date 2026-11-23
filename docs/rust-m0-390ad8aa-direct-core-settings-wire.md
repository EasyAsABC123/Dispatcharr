# M0 direct Core settings GET wire parity

Original Dispatcharr, React and dependency authors and contributors retain full credit.

The fresh Docker Compose comparison passed all eight signed GETs per arm for Standard and Administrator. Original and Native returned HTTP 200 and identical JSON bytes, including field order. Both supplied a single `Allow` field and a single `Vary: Accept, origin` field. The audit decoded and rehashed all 16 retained response bodies, checked MIME and literal header values and multiplicity, and separately verified the typed responses, actor identities and unchanged scoped SQL and broker state.

| Route | Bytes per response | Literal Allow |
| --- | ---: | --- |
| `/api/core/useragents/` | 851 | `GET, POST, HEAD, OPTIONS` |
| `/api/core/useragents/{id}/` | 206 | `GET, PUT, PATCH, DELETE, HEAD, OPTIONS` |
| `/api/core/streamprofiles/` | 1,064 | `GET, POST, HEAD, OPTIONS` |
| `/api/core/streamprofiles/{id}/` | 118 | `GET, PUT, PATCH, DELETE, HEAD, OPTIONS` |

The preceding `585fde95` run matched typed JSON and byte lengths. Native omitted `Allow`, varied only by `origin`, and inserted `is_active` before the Source User-Agent and Stream Profile positions. It also inserted `locked` before `user_agent` in Stream Profiles. Reserializing the same typed rows in each arm's field order reproduced all 16 earlier body hashes. The correction applies the Original order to successful direct JSON GETs and lets Main append the existing `origin` variation.

The ordinary AMD64 package at `390ad8aa16f07de7cbc7364a86f1eb2254faa4c8` passed the focused host Rust test and Main compile. The audit rehashed all 2,161 archived inputs and compared modes to the predecessor. The archive changes only `rust/crates/store/src/core_reads.rs` and preserves all 497 React, license and attribution inputs. The physical run used pinned Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and the exact recorded Native image. The fixture used an internal network with no published ports. All literal phases passed, and cleanup confirmed the baseline, candidate, data-init and origin container/process generations absent and empty owned container, network and volume inventories. [Sanitized evidence and hashes](../scripts/rust-integration/m0-390ad8aa-direct-core-settings-wire.json)

This covers successful JSON GETs on the four direct routes. HEAD and OPTIONS execution, content negotiation, other methods, failure responses and wider role coverage remain open, along with formal M0 and performance acceptance.
