# M7 Connect integration and delivery-log reads

Packaged Rust `d426d322b27a197aaaaa057ef73dfb53c516c452` and pinned Original Dispatcharr `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` each passed 12 administrator GET observations in isolated Docker Compose projects. Both returned HTTP 200 JSON throughout. The physical pair and its independent reducer each exited 0.

- The integration list matched when empty and after seeding two integrations with nested subscriptions.
- Delivery history matched when empty and after seeding four ordered entries. The checks covered first, second, and last pages, previous and next links, type filters, integration filters, combined and empty-result filters, and a paginated filtered result.
- The checked JSON projections matched after normalizing generated fixture IDs and local origins. The public receipt retains the projected body hashes and visible row counts.
- Owned SQL stayed unchanged through the reads. Exact cleanup verified that two Original roles and four Native roles and their recorded host generations retired, with empty Compose project inventories.

The fixture seeded delivery history and had opt-in jobs disabled. These observations cover the tested read paths. Delivery execution, live event propagation, performance, and the formal acceptance matrix retain their separate gates.

[Machine-readable receipt](../scripts/rust-integration/m7-d426d322-connect-positive.json)

Original Dispatcharr and dependency authors and contributors retain full credit.
