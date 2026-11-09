# Stream management compatibility

Original Dispatcharr authors and contributors retain full credit. These contracts
derive from `apps/channels/api_views.py`, its serializers/models/signals and
the pinned original revision recorded in each integration report.

The [combined 518-check image](../rust-integration/verified-integration-m5-full-518.json)
includes 102 stream read/write checks plus the earlier account/channel suites.
The isolated stream run previously passed 120 checks including the 18 shared
schema/auth/media checks. Neither result establishes complete M5 compatibility
or application performance gains.

Verified behavior includes standard/admin URL visibility, always-paged default
lists, filter/order/page selection, inactive-account exclusion, the `ids` list
override bypassing ordinary filters, custom creation hash signals, readonly
provider fields, custom-flag transitions, partial/full updates, null/empty
relations, source URL-validator quirks, timestamps/stat JSON numeric types,
bulk deletion and channel catchup/stream-link cleanup. Golden probes compare
canonical JSON digests, preserving numeric types. Independent lifecycle IDs,
correctly derived custom hashes and validated recent automatic UTC timestamps
are the only generated stream values normalized.

The next source-driven slice adds channel row expansion and stat deltas:

| Route | Source behavior | Evidence status |
| --- | --- | --- |
| `GET /api/channels/channels/{id}/streams/` | Channel level/adult/profile ACL; no hidden-output filtering; streams in membership order including inactive providers; standard-user URLs omitted | Passed immutable-image source comparisons |
| `GET /api/channels/channels/{id}/streams/stats/` | Same ACL before parameter validation; model updated-at descending order; strictly newer `since`; optional comma-separated integer IDs; only ID/stat/time fields | Passed immutable-image source comparisons |

The extended existing streams suite checks role restrictions, both orderings,
inactive-provider inclusion, null stats, strict timestamp boundary, combined
filters, empty/invalid/large integer IDs and missing-channel precedence. Unit
tests check integer parsing. The extended 127-check stream suite passed within
the [419-check focused run](../rust-integration/verified-integration-m5-startup-relations-1.json). The
usual ISO forms are supported. Unusual Python datetime/integer parsing forms
still need broader source vectors before claiming complete parser equivalence.

Remaining stream work includes multipart upload/storage lifecycle, preview and
other actions, uncommon malformed filter/bulk error bodies and source HTML
exceptions. Provider refresh, credential transforms/pools, EPG/DVR jobs and
realtime event delivery remain separate migration gates. Unsupported operations
must not report success without their required side effects.
