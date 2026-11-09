# Provider and EPG management read migration

All application credit belongs to the original Dispatcharr authors and
contributors. Authoritative contracts are `apps/m3u/api_views.py`,
`apps/m3u/serializers.py`, `apps/epg/api_views.py`, `apps/epg/serializers.py`,
their model metadata and the shared account permission map.

The unchanged React `initData` requests accounts, server groups, EPG sources and
EPG data. These additive routes read the existing schema; they do not create
tables or claim to execute imports/scheduled refresh jobs.

| Resource | Read routes | Serialization details |
| --- | --- | --- |
| M3U accounts | `/api/m3u/accounts/`, `/{id}/` | Nested profiles, sorted filters, through-group settings/counts, cron, VOD/group defaults, hash-key override, default/earliest/all expiration fields; password only for admins |
| Account profiles | `/api/m3u/accounts/{parent}/profiles/`, `/{id}/` | All profiles including inactive/default, exact basic account object, microsecond UTC expiration |
| Account filters | `/api/m3u/accounts/{parent}/filters/`, `/{id}/` | Scope by parent; standalone order remains source model order, account-nested filters sort by order |
| Server groups | `/api/m3u/server-groups/`, `/{id}/` | Only ID/name |
| EPG sources | `/api/epg/sources/`, `/{id}/` | Password always omitted; cron/count/has_channels fields; original raw channel EPG mapping controls has_channels, so override-only mappings do not count |
| EPG data | `/api/epg/epgdata/`, `/{id}/` | ID/tvg_id/name/icon/source only |

Every read requires an active standard-or-admin user plus the original UI
network policy. The source viewsets configure no pagination, search backend or
filter fields, so even `page`, `page_size`, `ordering`, `search`, `is_active`
and EPG-source query parameters are ignored. No artificial result limit is
introduced. Missing nested parents return empty lists; nested details remain
scoped to the parent. Permission checks precede object lookup. Arbitrary-size
integer keys outside PostgreSQL's bigint range return the original empty-list
or model-not-found result, preserving the distinction from malformed keys.

Account SQL pre-aggregates profiles, filters, through groups and group stream
counts. Detail queries restrict those aggregates to the requested account.
There is no round-trip-per-profile/group query loop. Legacy through-row JSON
properties reuse the original JSON-string-to-dict and Python integer-list
coercion behavior, including booleans and truncated floats. The same correction
is applied to the existing channel-group provider serializer.
The initial image5 trial exposed eight list-order failures despite identical
values by ID. Repeated original reads stayed stable. Corrected SQL retains the
original interval/crontab join projection and base row ordinals before joining
aggregates, with through rows scoped to the same selected parent IDs as source
prefetch. Tests are not sorted or relaxed. Actual PostgreSQL/ORM diagnostic
queries confirmed matching list/detail/group order. The subsequent immutable
image6 [real HTTP run](../rust-integration/verified-integration-m6-provider-reads-image6-1.json)
passed all114 checks, including96 provider-specific contracts. Prior failures
and diagnostic/cleanup hashes remain recorded.

Verification state: after matrix3's quiet window, the final module passed 41
store unit tests (including nested route registration), strict all-target Clippy,
22 probe/helper tests and 14 integration-runner tests. The source fixture syntax
compiled without executing mutations. The same image then passed all751 default
regressions and38 isolated proxy checks (20 additional proxy-specific contracts).
All disposable projects and retained diagnostic source directories were removed.
No performance gain is claimed by these compatibility tests.

The source fixture creates metadata through bulk ORM operations to avoid
external import signals. It uses only example.invalid URLs and a disabled
PeriodicTask. Profiles include active/inactive/default expirations and filters
with differing orders; raw versus override-only EPG links exercise the source
has_channels quirk. Fixture setup remains idempotent for retained-project use.
Usage sources deliberately disagree: the profile DB current_viewers is1,
legacy Redis profile_connections is7, and user_info.active_cons is the string11;
profile max_streams3 also differs from user_info.max_connections13. The source
serializer reads the model fields directly. Its get_active_connections and
get_max_connections helpers are not serializer fields and are not substituted.
Group stream counts are2/1, and nested integer/fraction/integral-float/large JSON
numbers retain exact source types after PostgreSQL storage.
Golden reports contain status/scalar/hash data only, never provider passwords,
profile credential properties or complete response bodies.

Reproduce with an immutable candidate and `--contract-suites provider_reads`
(96 provider checks plus18 shared foundation), then the complete management
suite. Account/profile/filter/source writes remain explicit 501
after admin authorization until their implementation and effects are verified;
EPG data retains its readonly contract. Uploads, refresh/account-info actions,
program/grid/current-program reads, Schedules Direct, provider credential pools
and native import/scheduling work remain separate gates.
