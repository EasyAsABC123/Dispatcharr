# Existing schema and authentication port

Derivative Dispatcharr backend work; original application credit belongs to
the Dispatcharr authors and contributors. See the repository's ATTRIBUTION.md
and retained AGPL license.

`Store` opens a bounded PostgreSQL pool against the existing migrated Django
schema. It does not create or migrate production tables. `AuthService` exposes
the existing token/login/refresh/logout paths and implements `PlaybackResolver`
for the initial native TS HTTP slice.

Implemented authentication includes Django PBKDF2-SHA256/SHA1 password checking,
HS256 SimpleJWT claims, access/refresh separation, inactive-user rejection,
API keys, trusted-proxy client-IP resolution, and global/per-user network rules.
PBKDF2 runs in blocking workers behind four permits; input/hash lengths and
iteration counts are bounded. Unknown password formats fail closed. Missing
credentials on the native TS path intentionally remain anonymous because the
upstream path is AllowAny with a STREAMS network gate. Presented credentials are
validated before the channel can be reused; admin operations require an active
admin-level user and UI network permission.

The playback resolver reads existing channel overrides, profile defaults,
ordered streams and active provider profiles. Initially it supports only the
locked native Proxy profile and Standard providers with identity transforms.
Other profiles/protocols/credential transforms fail explicitly. Capacity limits
are passed to the HTTP engine; they must be reserved atomically before upstream
input starts. Per-user stream limits are enforced by the engine's distributed
client leases; admission fails closed when the required coordinator is absent.

Current unit tests check PBKDF2 Python vectors, Unicode/legacy hashes, malformed
and oversized hashes, JWT signature/type/expiry behavior, forwarding trust,
local throttle behavior, and anonymous credential handling. Real migrated-schema
and bidirectional Python/Rust JWT checks run in the repository's M2 Compose
integration runner; unit tests are not a replacement for those checks.

The account API slice includes user list/retrieve/me and admin create/update/delete,
group list/retrieve/create/update/delete, permissions, API-key
listing/generation/revocation, and owner PATCH-me updates.
User/group/permission lists and retrieval require admins; me and own API-key
operations accept every active authenticated user passing the UI network gate.
Targeting any explicit user ID for a key requires admin permission, matching the
original action. User JSON omits password and preserves the original serializer's
API-key, channel-profile, nullable JSON, and UTC timestamp shapes.

PATCH-me strips privileged fields and admin-managed custom properties. Custom
properties merge under a PostgreSQL row lock and null values delete individual
keys. Password updates use the baseline Django cost of 1,200,000 PBKDF2 rounds
and bounded blocking workers. The current email validation is a conservative
implementation for ordinary addresses; complete Django validator edge-case
parity and setup/proxy sign-in still require their own source-driven tests before
M5 can pass. Administrative relation updates run in transactions and reproduce
the original channel-profile/group-permission sets. User deletion explicitly
removes known Django CASCADE dependents before deleting the user because the
original PostgreSQL foreign keys do not implement ORM collector behavior.

The owner/admin per-app storage routes implement namespace validation, compact
UTF-8 sizing, depth/NUL/value/body/user-total/app-count limits, per-user quota
locking, replacement and idempotent deletion. Python numeric equality and float
formatting are preserved for unchanged-value checks and byte accounting; large
JSON integers are retained without conversion through f64. Golden HTTP suites
exercise each implementation independently, including concurrent quota writers.
Their execution against real migrated tables remains required evidence, rather
than a unit-only completion claim.

Remaining compatibility work includes SQLite, non-PBKDF2 Django hashers and hash
upgrades, session/CSRF behavior, reverse-proxy sign-in, exact DRF parser/error
edge cases, audit/inbox events, and global Redis-backed authentication throttles.
In particular, the source's user-creation-without-password case crashes with an
uncaught KeyError despite declaring password optional. The Rust handler fails
closed with status 500 and a JSON diagnostic; it does not reproduce the source
HTML error page. That error-body difference is an unresolved compatibility item,
not a silently normalized passing contract.
The current bounded in-process throttle preserves the nominal 3/min login and
10/min failed API-key budgets but does not yet share budgets across workers or
Python. This crate is not a completed M5 account migration.

Core settings, user agents, stream profiles and output profiles now expose
existing-schema list/detail reads with standard-user and UI-network permission
checks. The 190-check immutable Compose run recorded in
[verified-integration-m5-core-profiles-2.json](../../../scripts/rust-integration/verified-integration-m5-core-profiles-2.json)
verified these reads plus channel-profile create/update/delete/duplicate,
default/empty memberships, user visibility and relation cleanup. The subsequent
[274-check run](../../../scripts/rust-integration/verified-integration-m5-memberships-channel-reads-1.json)
also verified single/bulk membership mutations and channel read serialization,
filters, pagination and visibility. Core mutation side effects and pending gates are
tracked in [CORE-CONTRACTS.md](../../../scripts/rust-account-contracts/CORE-CONTRACTS.md).

The latest combined immutable image passed all
[518 real-schema HTTP/media checks](../../../scripts/rust-integration/verified-integration-m5-full-518.json).
The same image subsequently passed the packaged React, HEAD and bounded shutdown
[runtime checks](../../../scripts/rust-integration/verified-runtime-shutdown-1.json),
after which its retained disposable fixture was removed. Stream-management scope
and remaining gaps are recorded in
[STREAM-CONTRACTS.md](../../../scripts/rust-account-contracts/STREAM-CONTRACTS.md).
The newer [655 default / 38 proxy checks](../../../scripts/rust-integration/verified-integration-m5-startup-relations-1.json)
verify startup helpers, configured setup, optional-IP-lookup-disabled environment,
trusted proxy sign-in, lightweight channel IDs/summary and stream/stat relations.
These additions are not included in the earlier 518-check result. See
[STARTUP-CONTRACTS.md](../../../scripts/rust-account-contracts/STARTUP-CONTRACTS.md)
for fixture policy and remaining gates. No application resource savings are
claimed by these tests.

Provider/account/profile/filter/server-group and EPG source/data reads are now
implemented as an additive M6 slice. Their immutable image6
[114 focused / 751 default / 38 proxy checks](../../../scripts/rust-integration/verified-integration-m6-provider-reads-image6-1.json)
passed after preserving source join/prefetch order; earlier failures remain
recorded. See
[PROVIDER-READ-CONTRACTS.md](../../../scripts/rust-account-contracts/PROVIDER-READ-CONTRACTS.md)
for exact role/password/default/cron/expiration/count behavior and the queued
golden gates. Job-triggering management writes remain explicitly unsupported.
The later [356-check run](../../../scripts/rust-integration/verified-integration-m5-channel-crud-2.json)
verified supported channel writes, stream ordering/catchup, raw/effective
overrides, group ownership and profile-selection creation side effects. Remaining
EPG/DVR/compact-numbering/stop-control gates are recorded
in [CHANNEL-CONTRACTS.md](../../../scripts/rust-account-contracts/CHANNEL-CONTRACTS.md).
The [405-check group/stop run](../../../scripts/rust-integration/verified-integration-m5-channel-groups-1.json)
then verified group CRUD, provider rename/demotion and actual active/inactive TS
stop/delete behavior, including a repeat against the replica image. Its focused
cleanup extension verifies unrelated-group protection and FK SET_NULL effects;
only the source's experimentally unordered deleted-name list is normalized.

An additional real-schema test is deliberately ignored by ordinary unit runs.
It is not counted as a passing check until explicitly run. Against a disposable
Django-migrated fixture database only, set `DISPATCHARR_STORE_TEST_FIXTURE=isolated`,
`TEST_DATABASE_URL`, `TEST_DJANGO_SECRET_KEY`, `TEST_DJANGO_ADMIN_ACCESS_TOKEN`,
`TEST_CHANNEL_UUID`, and `TEST_UPSTREAM_PREFIX`, then run:

```sh
cargo test --manifest-path rust/Cargo.toml -p dispatcharr-store --test postgres -- --ignored
```

Missing fixture values fail the opted-in test; no missing-database path is treated
as a pass. This test reads the fixture schema and credentials without creating
or modifying accounts. Real HTTP mutation tests belong to the Compose integration
suite and must use disposable databases.
