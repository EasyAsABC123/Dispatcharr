# M5 paired ARM64 React account workflow for d2c5e611

Original Dispatcharr revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and the Rust ARM64 package at `d2c5e6113efe947551c17bf877e8c6b2c964190c` completed the same ordinary Chromium workflow in separate fresh owned Compose stacks on Lappy. Chromium rendered the setup form, created the initial administrator, signed in, rendered Channels, opened the account dialog and signed out. Each database recorded a single active administrator and one setup mutation, and settings and migration records stayed unchanged.

Each browser recorded 30 HTTP requests with the same status counts, 28 responses at 200 and two at 401. Both recorded zero blocked off-origin HTTP or WebSocket requests and zero page errors. The same immutable frontend image supplied both arms. The 112-file asset manifests matched byte for byte and each origin served the same index hash.

The owner, both application test arms, both Chromium processes and the separate read-only audit exited 0. Each browser closed its context and retired its temporary profile. Exact Compose cleanup accounted for seven Original and six Native containers, two named networks per arm and one volume per arm. The audit matched each post-down lookup to its typed absence and checked the empty filtered inventories. Raw browser assets, request details and credentials stayed on Lappy.

This result covers the ordinary initial-administrator account workflow. Secondary account roles, settings-page behavior and full M5 remain open. The original Dispatcharr authors and contributors retain full credit for the application and React frontend.

[Machine-readable receipt](../scripts/rust-integration/m5-d2c5e611-arm64-account-browser-pair.json)
