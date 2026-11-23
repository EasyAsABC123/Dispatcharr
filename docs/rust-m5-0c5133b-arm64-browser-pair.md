# M5 current paired ARM64 React account workflow

Original Dispatcharr and ordinary Rust application `0c5133b` passed the
same Chromium workflow on Lappy. Starting from disposable empty
storage, the browser completed first-administrator setup, signed in,
opened the account dialog, and signed out. Both arms passed six
browser steps and the scoped persisted account checks.

Original used its pinned 1,211-file source overlay and its stock runtime
entrypoint. Rust used its exact native ARM64 package. The browser used
the same observer and scenario on both arms. Both applications and
browser observers exited 0, and exact Compose and browser cleanup passed.

The result qualifies the ordinary account workflow and unchanged React
bundle on ARM64. Wider React/API coverage and the AMD64 retry remain
open. Original Dispatcharr, its React frontend, and its dependencies
retain their existing attribution and licenses.

[Machine-readable receipt](../scripts/rust-integration/m5-0c5133b-arm64-browser-pair.json)
