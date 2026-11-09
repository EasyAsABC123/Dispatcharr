# Core management compatibility gates

All source behavior belongs to the original Dispatcharr authors and contributors.
See [ATTRIBUTION.md](../../ATTRIBUTION.md). This is a migration work record, not a
claim that the core management milestone is complete.

The source contracts are `core/api_views.py`, `core/serializers.py`,
`core/models.py`, `core/signals.py`, and `apps/accounts/permissions.py` at the
pinned original revision. Core settings, user agents, stream profiles and output
profiles expose list/retrieve for active users at level 1 or higher, and mutations
for administrators at level 10 or higher. Both also enforce the UI network rules.
A level-zero user cannot read these management APIs even though it can read its
own account. Missing and malformed detail IDs are checked after permissions.

The Rust store now implements these four list/detail reads against existing
PostgreSQL tables. `run_core` adds 32 source-versus-candidate probes when each
reference collection has at least one row (28 minimum when all are empty).
The 190-check immutable run documented in
[verified-integration-m5-core-profiles-2.json](../rust-integration/verified-integration-m5-core-profiles-2.json)
passed these reads and channel-profile CRUD/duplicate oracles. The earlier
135-check image predates these routes. The first 190-check run found and corrected
invented collection ordering and malformed-ID error bodies; its report hash is
preserved in the sanitized record.

Write work must include the source side effects before declaring parity:

| Resource | Required behavior beyond row fields |
| --- | --- |
| Settings | Unique key and model-required validation; EPG history clamping; system log value clamps; strict network CIDR validation on updates; reverse-proxy header checks; DVR series rule sanitation. |
| Stream settings | Acquire account rehash locks before changing effective hash fields; exact 409 blocked-account response; enqueue rehash with preacquired token; release locks on validation failure. |
| DVR settings | Reschedule future recordings after pre/post offset changes or creation with offsets. |
| Core settings saves/deletes | Invalidate Django Redis group and version cache keys; apply log/display time zone changes; network notifications and condition-cache synchronization. |
| User agents | Uniqueness of both name and header; auto timestamps; default-header cache invalidation; Django SET_NULL foreign-key cleanup on deletion. |
| Stream profiles | Existing locked profiles permit only user-agent changes, checked in source model field order; deletion rejects locked entries with source list-shaped validation error; relation cleanup. |
| Output profiles | Existing locked profiles reject every field change; deletion rejects locked entries; successful deletion scrubs user and HDHR output-profile references. |

CRUD probes must use disposable `rust-probe-*` resources and restore shared
baseline state before exercising the second implementation. Match deterministic
bodies exactly; normalize only declared generated IDs/timestamps. Verify cache,
lock, job and dependent-row effects separately from HTTP response parity. Do not
capture proxy headers, passwords, API keys, tokens or settings values in evidence;
record statuses and canonical JSON digests.
