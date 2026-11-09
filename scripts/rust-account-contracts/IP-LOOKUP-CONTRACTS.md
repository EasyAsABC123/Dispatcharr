# Environment IP cache and bounded background lookup

All original application credit belongs to Dispatcharr's authors/contributors;
see ATTRIBUTION.md. This port derives from core/api_views.py and the original
CoreSettings grouped-cache helpers. Browser workflow and native WebSocket
transport require their own M7 gates.

The enabled-default environment endpoint has an actual original-source/native
HTTP comparison with real migrated PostgreSQL, Redis and private deterministic
providers: 18 cases cover cold/fresh/stale/lock-held state, coalescing, provider
HTTP/JSON failures, numeric/zero IP values, retained known data, freshness bumps,
legacy enabled-value types, disabled flags, exact request agent and cookie
isolation. The fixture redirects only the original provider destinations and
local UDP probe; no real public-IP/geolocation requests are made.

The worker follows original ordering: ipify JSON, local IPv4 UDP connect without
sending a packet, public-IP validation, primary geolocation and fallback only
on non-200 primary status. A primary transport/JSON exception does not trigger
fallback. An invalid whole ipify object abandons the worker and leaves its
original 30s lock until expiry. A failed recheck retains the previous complete
cache object and only advances verified_at, with no update publication. First
negative results are cached and published so the UI skeleton can resolve.

Django cache keys and formats remain shared: result `dispatcharr:ip_lookup_result`
TTL 3600, add-only `dispatcharr:ip_lookup_lock` TTL 30, reverify interval 60. The
private wire keys include Django's `:1:` prefix and primitive protocol4 pickles.
`verified_at` remains cache-only. Worker updates use the full original
`update`/`ip_lookup_complete` envelope via the M7 UpdateEventSink. The readonly
WebSocket helper removes verified_at without modifying the cached object.

Each provider call uses a new Requests-compatible Session/cookie jar, 5s connect
and read timeouts and the pinned request headers. Cookies survive redirects
within that one call and never cross provider calls. Decoded JSON is capped at
64KiB, the complete job at 30s, and one local worker is allowed alongside the
shared cross-replica lock. These resource bounds reject oversized/slow hostile
provider replies; their arbitrary payload equivalence is not claimed.

System settings honor shared cache/versioning and original DB fallback. An
empty/nonobject/missing DB row uses the complete 9-field original defaults, not
an empty cached dictionary. Already cached dictionaries are retained as-is.
The default-map correction was found after image13 capture and is separately
verified by the 18-case source comparison with complete group-value assertions;
it is outside image13's nonempty migrated-settings proof.

The frozen application gate is separately selectable as `environment_ip`:
fresh positive/negative cache and locked-cold default-enabled responses for
admin/standard/basic/inactive/anonymous callers. Every setup retains the shared
lock, preventing public-provider execution. This gate establishes package
routing/default-enabled cache behavior; the worker's actual private HTTP
execution is established by `run_ip_lookup.py`, not silently inferred from it.

Remaining gates include corrected empty-row defaults in a new immutable image,
native M7 update/replay and complete unchanged-React browser initialization,
additional charset/oversized-provider boundaries, and configured M8 integrations.
No application performance gain is inferred from these API/cache tests.
