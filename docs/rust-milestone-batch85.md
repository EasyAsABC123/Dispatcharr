# Channel editor guide links and modular PostgreSQL TLS

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit.

The existing React channel editor calls Set EPG before updating the channel.
The Rust action links or clears the guide field without changing the channel's
update timestamp. It performs the scoped admin lookup before reading the
request and uses the existing guide invalidation and programme publisher.
Dummy guide links and removal use cache invalidation. Non-dummy links require
the selected worker publisher. That publisher is opt-in in the current Main
package. The default package still needs an equivalent non-dummy workflow.

Main and the database operator accept an omitted PostgreSQL password in the
Original's `DISPATCHARR_ENV=modular` and `POSTGRES_SSL=true` configuration.
Explicit passwords and database URLs continue to take precedence. The local
Original entrypoint controls and SQLx parser comparison exercise this
configuration. The packaged Compose check authenticated to the real PostgreSQL server with
certificate-only configuration.

Formatting and strict all-target Store and application Clippy passed. The
Store suite passed all 530 default cases, with 79 external-fixture cases
ignored. Main and the operator each passed seven configuration tests.

The exact Docker build for `85d7db9b3` succeeded with frozen runtime packages
and all 497 React, license and attribution inputs matching Candidate84. The
default Set EPG comparison passed 22 Original and 21 Native checks with exact
cleanup. This includes the default package’s refusal before mutation when a
non-dummy guide publisher is unavailable.

The modular TLS fixture passed all six cases. Main reached HTTP 200 with
client DN `/CN=dispatch` and the operator completed its SQL-backed command.
Both predecessor and both missing-client-certificate cases exited 1 as
expected. Storage and cleanup checks passed.

The first selected-worker fixture exited 1 before any business check.
The candidate rejected its configured version endpoint at startup. The
fixture needs the same owned loopback version endpoint as the existing
selected worker bootstrap. Cleanup passed. Selected EPG publication and
performance remain open.

[Focused results](../scripts/rust-integration/milestone-batch85/focused.json)
· [Docker build and image binding](../scripts/rust-integration/milestone-batch85/packaging.json)
· [Compose results](../scripts/rust-integration/milestone-batch85/packaged.json)
· [Milestone ledger](rust-milestones.md)
