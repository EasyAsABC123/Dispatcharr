# ARM64 ordinary package and signed-auth smoke

The published application `381a6a157d1f860527906794bb8c63859fcc8176` built and ran as an ordinary Linux ARM64 image on Lappy. The ARM64 image index was `sha256:dbbfb2245f8bd8619aa6522ba8218fa819326043eeda8f61ebb1861830d31bfd` and its build receipt SHA-256 was `4ca7a077d39cfd9e4970c981cae73343858ed180f1bd19d4f0524f078721cb11`. The independent source comparison matched all 2,178 application inputs to the verified AMD64 package, including 1,681 Rust paths and 497 preserved React, licensing and attribution inputs. Both packages used the same application source scope.

The fresh internal Compose fixture returned HTTP 200 for readiness and the React entry, empty setup, first-admin creation, setup closure, signed JWT login and the authenticated channel groups request. The fresh schema had 62 public tables and 184 applied migrations. The run created one active administrator and left the migration and channel-group counts unchanged. The application ran as UID 65532 with ordinary package flags. Each package, runtime and proof phase exited `0`.

The same-host independent audit verified absence of all five owned containers, the network by ID and name, and the named volume. The signed-auth receipt SHA-256 was `dbaf1e60a1cd68072cba228e72b18b9d1eb9e453942158f01ecbe82c37e3819b`. The private test used neither host publication nor an image push. This qualifies the exact ARM64 package and fresh auth flow. ARM64 signed inventory and guide comparison, performance acceptance and full rollout remain open.

Application and React credit belongs to the original Dispatcharr authors and contributors.
