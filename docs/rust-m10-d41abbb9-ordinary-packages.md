# Ordinary AMD64 and ARM64 packages on d41abbb9

The ordinary packages built and verified successfully on Linux AMD64 and ARM64. Both matched application source tree `6362c9ead4c4532bc54132965c98ce969f73465d4bdb8adce802a66d9399aeb1` for all 2,187 inputs. The three Rust changes preserved the 497 React, attribution and license inputs and all source modes.

| Platform | Exact image | Physical verification |
| --- | --- | --- |
| Linux AMD64 | `sha256:d242cc95abdb9cd24e54a69b835057df4e18c626d41edaf70737eda3a5bf3380` | Default build, frozen runtime packages and same-image signed Notification Center replay |
| Linux ARM64 | `sha256:e27ee44878df3d26160846e3fa11f0db33c24ecd1c7fa9419d3faa8fe25992a0` | Offline build, React entry, signed administrator login and channel-group read |

The AMD64 owner, preflight, build and verification exited 0. The ARM64 build, signed smoke, proof and owner exited 0. The ARM64 fixture admitted one synthetic administrator, returned HTTP 200 for login and the signed channel-group read, and retired five exact container identities. Its project container, network and volume inventories were empty after teardown.

The matching host run passed formatting, strict Clippy, three focused wire tests and 1,750 workspace tests across 181 suites. It reported zero failures and 821 ignored. Five Timeshift goldens were bound as host-only test support.

The two package receipts cover the ordinary defaults and their listed smoke tests. Full release acceptance and performance remain open. Original Dispatcharr and dependency authors retain full credit and licenses. [Package evidence](../scripts/rust-integration/m10-d41abbb9-ordinary-packages.json) · [AMD64 notification evidence](rust-m0-d41abbb9-amd64-notification-methods.md)
