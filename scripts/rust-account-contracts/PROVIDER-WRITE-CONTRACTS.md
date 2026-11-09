# No-job provider management mutations

All application credit remains with the original Dispatcharr authors and
contributors. These contracts derive from the M3U viewsets, serializers, models
and signal registrations. This is an incremental M6 slice, separate from the
verified 751-check read/source-compatibility checkpoint.

ServerGroup CRUD and scoped M3UFilter CRUD are now implemented and wired. The
module passed43 store unit tests, strict all-target Clippy, coherent workspace
compilation and25 Python helper tests. Its immutable image8
[85-check source comparison](../rust-integration/verified-integration-m6-provider-crud-image8-1.json)
passed, including26 ServerGroup and41 filter contracts plus18 shared foundation.
The same immutable image then passed all818 default regressions; disposable
projects were removed. No ingestion-performance or broader
native ingestion/Timeshift-idle acceptance is claimed by these API tests.

ServerGroup writes require an active admin and UI network permission. Names use
the source100-character limit, trimming, scalar coercion, uniqueness and
required PUT/create behavior. PATCH preserves omitted fields; readonly IDs and
unknown fields are ignored. Deletion reproduces Django collector SET_NULL on
accounts without calling Account.save, changing updated_at or scheduling jobs.
The26-case oracle validates identity before normalization and uses a restricted
source fixture helper to attach an owned inactive account without save signals.

M3UFilter writes have the same admin/UI gate and scope every detail to the URL's
account. The source serializer exposes only ID/type/pattern/exclude/order/JSON;
payload parent IDs cannot move a filter. Create defaults are group/true/zero/{};
PUT requires a pattern while preserving omitted optional fields. Patterns use
the source string/length validators. Model.clean compiles regex, but ordinary
ModelSerializer.save does not invoke full_clean; the queued source oracle
explicitly tests whether malformed regex is accepted, rather than assuming the
Django-admin form behavior. Actual Python/Rust HTTP lifecycles confirmed that
the API accepts malformed regex. Choice errors, bool/integer coercion, nullable/scalar
JSON and write-response versus subsequent JSONB number types are covered.

The future focused command is `--contract-suites server_group_crud,filter_crud`
against a fresh immutable image containing these handlers. Both independent
lifecycles create only rust-probe-prefixed fixtures and clean their exact IDs;
reports contain statuses, scalars and canonical hashes rather than bodies or
credentials. The default integration suite includes both new slices, so older
read-only images must use their captured tools or explicitly select supported
suites.

Remaining gates include actual HTTP proof, unusual malformed-parent/FK and
database-race HTML error behavior, broader parser/renderer edges, and account,
profile, EPG-source, upload/refresh/account-info mutations with their native
jobs/cache/notification effects. Those operations remain explicit501. This
slice does not complete M6 or enable unverified scheduler/import success.
