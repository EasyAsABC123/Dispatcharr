# Current AMD64 ordinary Main proxy health

Five fresh Compose cases ran against the exact ordinary Rust package at `2d3ffe53`. Both the default health policy and the dynamic opt-in policy made their expected decisions during a bounded upstream stall and recovered against the same origin. The default arm captured six reads in the stall window and the opt-in arm captured five. [Redacted proof](../scripts/rust-integration/m2-2d3ffe53-ordinary-main-health.json)

Three other cases held a real Redis reply at the transport boundary. When the reply was delayed, Main forwarded the reply and the signed reader received media. When the reply was dropped, the witness verified the same copied PostgreSQL instance and the exact blocker before the signed media recovered. With the reply held during a separate shutdown, Main stopped cleanly with exit 0. Every case retired its exact Compose resources.

The full Native proxy-health row still needs the component, ordinary HTTP subscriber and clock-boundary evidence.

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
