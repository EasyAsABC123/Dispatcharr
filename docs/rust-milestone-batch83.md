# Integration deletion and literal PostgreSQL credentials

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

Native Main accepts the existing React Integrations delete request under the
admin policy. It returns an empty 204 after deleting the target integration
and its dependent subscriptions and delivery logs in one transaction. The
pinned Original passed all nine scoped deletion cases with exact cleanup.

The PostgreSQL resolver now preserves literal percent sequences and reserved
characters from discrete configuration through SQLx connection parsing.
The explicit URL path retains precedence. The qualification reproduced the
previous decoding mismatch using the actual SQLx parser and checked the
corrected values against the pinned Original settings.

The exact Docker image matched nine deletion cases per backend and passed
the 15 integrated Connect lifecycle checks. The packaged operator passed
seven real PostgreSQL cases covering discrete credentials, explicit URL
precedence and incorrect credentials. Each scope independently verified
cleanup. The first deletion attempt stopped at its preflight. Its corrected
fixture used the same immutable image.

[Exact-image package checks](../scripts/rust-integration/milestone-batch83/packaged.json)

[Focused checks](../scripts/rust-integration/milestone-batch83/focused.json)
· [Original deletion capture](../scripts/rust-integration/milestone-batch83/original-delete.json)
· [Milestone ledger](rust-milestones.md)
