# Remaining Xtream / XMLTV read contracts

Full application credit remains with the original Dispatcharr authors and
contributors. The reference is commit
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`, using its actual HTTP views, original
PostgreSQL schema, Redis cache, and helper functions. This is an audit and gate
plan, not native acceptance.

`output-http` already owns `/get.php` and normal M3U output. Do not introduce a
second owner for that route. The next slice is `/player_api.php` and
`/panel_api.php` account information, live categories, and live streams. Other
player actions (stored/dummy EPG, movies, series) and XMLTV are separate slices.

These functions are ordinary Django views. They authenticate only the query
`username`/`password` through the original XC_API policy and shared per-IP Redis
throttle; a presented Bearer/API key does not add DRF authentication. Unsafe
methods encounter Django CSRF middleware before these functions. Player/panel
views themselves have no method decorator; HEAD still executes the work. The
actual URL declarations are unanchored regular expressions with an unescaped
period, so canonical-path acceptance must not imply every unusual alias.

Account information echoes query credentials, uses the original static output
format list, and always reports Active after XC authorization (which does not
independently require `is_active`). Positive user limits select that user's
connection count; otherwise it counts all users and uses the tuner calculation
with unlimited default 50. Live, timeshift, and persistent-VOD hashes all count.
The source returns zero connections on any outer Redis/counting exception.
Datetime fields and 90-day expiry are volatile and must be validated relative to
request time or through a fixed-clock actual-helper fixture. Passwords/tokens
stay in memory and are redacted from all persisted evidence.

Server-info host/scheme differs from absolute URL generation: Django
`SECURE_PROXY_SSL_HEADER` accepts the first X-Forwarded-Proto token independently
of the helper's trusted-proxy origin rules. `request.get_host()` normally uses
Host, then splits on the first colon. Explicit versus implicit ports and UTC
time fields require original HTTP comparisons.

Live category visibility uses base channel-group membership and minimum
effective numbers over visible, non-hidden channels. Live stream entries use
effective group/name/number/logo/radio fields, but category filtering occurs on
the base group. Every output user, including admins, is capped by user level;
non-admin adult and profile restrictions apply. Collision-free integer numbers
are assigned in two passes: preserve all whole numbers, then assign fractional
and null channels the next unoccupied integer. Filtered categories recalculate
the mapping. Catch-up advertising checks both system and exact-false user flags.
A missing effective group lazily creates the original Default Group. Read
acceptance must capture this database side effect, not invent a read-only result.

The initial fixture freezes created-at values, includes base/effective category
disagreement, assigned profiles with disabled memberships, adult and level-11
channels, a null number, a negative fraction, and a channel without a group.
Inactive XC users are intentionally included: the original XC credential helper
does not apply the active-JWT login restriction. Bad presented Bearer/API-key
headers remain independent of query authentication on these Django views.

Panel responses include empty series/movie category arrays plus the complete
live category/stream dictionaries. Unknown player actions return account info;
they do not silently become empty arrays for unported actions.

XMLTV uses `apps/output/epg.py`, `dummy_epg.py`, and
`streaming_chunk_cache.py`. It streams real programmes, standard dummy programmes,
and custom regex-driven dummy programmes. Its Redis single-flight chunk cache
has a 300-second ready TTL, 120-second build lease, generation invalidation, and
followers. The same XML builder backs normal `/output/epg` and XC `/xmltv.php`,
with different authorization, visibility, integer channel IDs, and blocked-event
logging. HEAD on normal EPG is cheap, whereas XC passes through generation.
Custom dummy matching uses third-party `regex`, not builtin Python `re`; existing
preview corpus/backend uncertainty must not be hidden by substituting an
incompatible engine. EPG ingestion/parser/database writes remain root-owned.

Native read work needs admission before authentication/Redis/database awaits,
owned cancellation-safe tasks, bounded metadata and response buffers, final
allocation-capacity leases, capped queued requests, and globally bounded drain.
Do not repeat the whole-list / repeated-origin amplification fixed in HDHR.
Required gates include actual original/native HTTP payloads and side effects,
role/profile/adult/category/override cases, all connection namespaces, malformed
and stale Redis rows, slow/dropped readers, retained body frames, cancellation,
and shutdown. No resource-performance claim follows before equivalent side
effects and a separate frozen paired measurement.

The pure numbering oracle `numbers_source.py` extracts the five original
assignment/loop statements from `_xc_live_streams_setup` using Python's AST. It
does not replace those statements with a second Python implementation. The
source file and extracted AST are hashed. The planned vectors include repeated
whole numbers (which remain duplicated), many null/fractional channels,
negative fractions, signed zero, finite values beyond i64, and rejected
non-finite values. All 392 original/native numbering vectors now match; see
`verified-xc-account-live-host-1.json` for the frozen host comparison scope.

Connection counting needs its own original-helper corpus in addition to HTTP
fixtures. Its `RedisClient.get_client()` explicitly enables decoded responses
(unlike the media-buffer client), so Python integer/float conversion receives
Unicode strings. Capture signs, Unicode decimal digits and surrounding space,
underscores, invalid UTF-8 Redis values, overflow-sized integers, NaN/infinity,
missing fields, malformed timestamps, and wrong-type hashes. Invalid user IDs abort
the entire user-filtered count; invalid timestamps skip only the matching row.
The all-user count bypasses user-ID parsing entirely. A bounded native scan
must finish before returning a count, so a late error cannot expose a partial
count. Capacity exhaustion is a separate explicit resource rejection, not a
successful zero-count response.

The lazy Default Group read side effect must also preserve existing rows and
relationships: no update or INSERT attempt for a group already found, no
reinitialization of an existing account relationship, and a concurrent unique
insert retry. The draft previously used an upsert that rewrote an unchanged
name; source inspection corrected that before any execution or acceptance.

Subsequent action families need separate gates. Movie/series categories and
lists select relations from active provider accounts, with winning priority and
ID tie-break rules, artwork rewriting, and per-user feature toggles. Category
names use PostgreSQL Lower ordering while final relation lists use Python
Unicode lower ordering; do not assume those collations are interchangeable.
Movie maturity filtering does not imply an equivalent series-list filter.
Detailed movie/series actions can synchronously invoke provider refresh tasks
after their 24-hour freshness checks and reload database rows. Cached-only
responses cannot be described as equivalent for those stale cases. Provider
HTTP, database mutation, and refresh-failure behavior must be coordinated with
the ingestion migration before those actions are enabled.

Verified host evidence in `artifacts/rust/m7-xc-native-6` covers 76 HTTP cases,
67 original Redis counter cases, 392 numbering vectors, and seven actual
Redis/PostgreSQL resource tests. The shared float parser separately matches
2,827 pinned Python3.13.18 / Unicode15.1 conversions. Missing-group defaults
come from the actual original getter in a rolled-back fixture transaction;
native creation matches with eight concurrent callers and owned-row restoration.
Native-1 exposed four JSON-spacing differences; native-2 was a test dependency
placement build error; native-5 exposed a fixture assumption about SQL cascading
(Django collects children in its ORM, but the SQL FK is NO ACTION). All failures
remain preserved; native-3 and -4 are earlier narrower successful gates.

Generated live URLs require a separate adapter: `/live/{user}/{pass}/{id}` and
the short credential route. Source `stream_xc` performs DRF
authentication before XC_API credentials/channel visibility, forces TS/fMP4/HLS
from its suffix, and delegates live playback with its resolved XC user while
skipping another global STREAMS policy check. This differs from the ordinary
Django player/panel handlers. Generated-live decode, quota/metadata/format
behavior, short-route precedence, and packaged shutdown remain required.

The adapter is now main-wired through the existing app media dispatcher, with
actual frontend assets/static directories preferred over the short alias.
Its frozen-host `m7-xc-live-native-2` gate passed all 36 captured Source wire
cases plus the existing seven SQL/Redis resource tests. Allow token order is
normalized because the Source decorator constructs that header from a Python
set. Native-1 preserved 21 initial renderer mismatches: ordinary missing-channel
errors use spaced Django JsonResponse, while admin missing-channel errors use
DRF's compact `detail` shape. These are fixed in native-2. The component peer
does not contain live engine services: positive playback is still unverified.
`image.py` is prepared to compare both HTTP families, construct a TS URL from
the actual catalog, decode H264/audio, check nested-static routing, and verify
packaged shutdown against the next Root-built immutable app image.

Exact image21 passed `m7-xc-image21-4`: 112 Source/native HTTP families, catalog
stream1's generated credential URL returning actual H264/AAC TS, 74 decoded
video frames with zero decoder stderr, zero packet sync errors, nested static404
without XC interception, and shutdown0.207s/exit0/noOOM/no drain warning. Cleanup
passed. Image/source digests and proof links are in `verified-xc-image21-1.json`.
Runs -1/-2/-3 remain preserved as probe import-priority, empty-static404 JSON
decoding, and local import-shadow failures. None required runtime changes.
Positive HLS/fMP4 suffix/user-policy variants and the remaining action families
are still excluded; this functional result establishes no resource gain.
