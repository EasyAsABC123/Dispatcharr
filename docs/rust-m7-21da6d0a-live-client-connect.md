# M7 live MPEG-TS client Connect delivery on 21da6d0a

Pinned Original and the exact Rust Main package each ran one
authenticated raw MPEG-TS viewer using the existing enabled plain
Connect webhook settings. The audit was visible after the first 75,200
aligned media bytes. Each arm recorded one linked SystemEvent, one
receiver POST, one successful durable DeliveryLog and one upstream
connection. Viewer closure naturally retired the upstream. Both
fixture projects verified owned roles and process generations absent.
The exact package, pair controllers and separate reducers exited 0.

Rust uses the existing saved enablement, shared Auth writer and Connect
delivery path. It writes the audit before yielding useful media and
continues the webhook operation outside the streaming task. No new
configuration flag is required. Current readiness eligibility adds a
PostgreSQL lookup on the first useful raw authenticated MPEG-TS path.
No-listener audit coverage, other live transports, script and template
subscriptions, whole M7 parity and performance remain open.

[Machine-readable receipt](../scripts/rust-integration/m7-21da6d0a-live-client-connect.json)

Original Dispatcharr and dependency authors and contributors retain full credit.
