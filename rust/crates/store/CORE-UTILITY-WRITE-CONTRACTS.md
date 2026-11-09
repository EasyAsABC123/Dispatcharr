# Core helper reads and StreamProfile writes

All application design and original source credit belongs to the original
Dispatcharr authors and contributors; see the repository ATTRIBUTION.md and
AGPL license. This is a compatibility port of their implementation.

This slice is **verified against the original source on immutable image10**:
94 focused checks and 894 full-regression checks passed. The image/source/report
bindings are recorded in `scripts/rust-integration/verified-integration-m6-core-image10-1.json`.

- StreamProfile list/detail retain existing standard-user read permissions.
  Create/update/delete require an active admin and the original UI network ACL.
  Name is required on create/PUT, duplicates are allowed, and optional omitted
  fields survive PUT. Command/parameters remain stored strings; API writes do
  not execute them. Readonly identity/unknown fields are ignored.
- Locked profiles allow user-agent changes and no-op writes. All other edits
  return the original list-shaped 400, choosing the first changed field in
  model field order. Locked deletion returns the original protected-profile
  error. Ordinary deletion sets Stream, Channel, ChannelOverride and M3UAccount
  profile FKs to NULL without calling related save hooks or changing timestamps.
- Timezones require any active authenticated user, including basic level zero.
  The 433 identifiers come from pytz 2026.6 in the exact original pinned Docker
  image. The checked-in dataset and provenance bind its bytes; host OS timezone
  data is never consulted. Region grouping follows the original sorted list.
- Stored system-event reads require admin and UI ACL, serialize original event
  display names, preserve unknown display names, use timestamp order and Python
  isoformat UTC `+00:00`, and cap positive pages at 1000. Bad query parameters
  return the source generic 500, including negative slice boundaries. Negative
  limit with a nonnegative stop yields an empty result, as Django slicing does.
  Existing database events are read; this slice does not claim native event
  generation or retention-job parity.

Queued golden suites are `core_utilities` and `core_stream_crud`; both are
included in the full integration default. Each backend creates distinct owned
fixture IDs, normalizes only its created object's ID, and compares status and
canonical response digests. Source-only fixture preparation links four owned
relations without triggering account/import jobs, and verifies NULL clearing
and unchanged timestamp hashes after deletion. Locked temporary rows remain in
that disposable database until Compose teardown because their public API
correctly forbids deletion. No credentials, command strings, or event details
are recorded in evidence.

Remaining edges include unusual parser/HTML failures, Unicode decimal query
integers, concurrent FK/unique races and unsupported core/output-profile job or
cache side effects. OutputProfile and UserAgent mutations are outside this
slice. Their source signals must be ported and tested before enabling writes.
