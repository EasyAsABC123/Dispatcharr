# Delivery Logs, the TS 404 response and Redis TLS

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

- Native Main now serves the admin-only Delivery Logs collection used by
  React. The read retains the Original type and integration filters, paging
  defaults, nested subscription, and request and response payloads within
  the existing metadata budget. The count and page use one read snapshot.
- A direct TS preview for an unknown stream now emits the same JSON 404
  response that Original returned in the paired control.
- When the configuration requests Redis TLS, the resolver rejects a
  contradictory plaintext URL. Enabling the underlying Redis TLS transports
  and certificate options remains its own compatibility requirement.

All 522 default Store tests passed, with 79 fixture-bound cases ignored.
All four focused Redis configuration tests and strict all-target Clippy
for Store and the application passed. The paired collection and Delivery
Logs run will also exercise the TS response through the packaged image.

[Focused checks](../scripts/rust-integration/milestone-batch79/focused.json)
· [Candidate78 packaged results](rust-milestone-batch78.md)
· [Milestone ledger](rust-milestones.md)
