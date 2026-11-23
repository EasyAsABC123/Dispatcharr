# 8a547cf0 ARM64 Standard-account browser parity

Original Dispatcharr at `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and the exact Rust `8a547cf00cd33a94fe4ccb982272be1861d52e73` ARM64 package passed the same real Chromium Standard-account workflow on Lappy. Each started from a separate fresh database, used ordinary administrator setup, and created one synthetic Standard account through the signed admin endpoint with HTTP 201.

The Standard account used the normal login form in its own browser context. Both arms returned HTTP 200 for password login, same-session self-profile, the Channels request and signed logout. Each rendered Channels, showed the role-specific navigation, and returned to the login form after logout. Both retained one administrator and one active nonstaff, nonsuperuser Standard account with an observed login. Settings and migrations stayed unchanged. Neither Standard session made an administrator account list request or an application write outside the recorded authentication lifecycle.

The paired trace distinguished the unauthenticated proxy availability 401 and bootstrap logout 401 before password login from the same-session explicit logout 200. Both browsers used the same 112-file React asset manifest and showed zero page errors or off-origin blocks. Owner, both arms, both Chromium runs and the independent audit exited 0. Browser contexts and temporary profiles retired. Exact teardown verified seven Original and six Native containers, two networks and one volume per arm, with empty scoped inventories.

This qualifies the ordinary Standard-account workflow on the exact ARM64 package. Further roles, broader settings and formal M5 remain open. The original Dispatcharr authors and contributors retain full application and React credit.

[Machine-readable receipt](../scripts/rust-integration/m5-8a547cf0-arm64-standard-account-browser-pair.json)
