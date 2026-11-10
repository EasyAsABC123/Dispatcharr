# React VOD REST inventory and Source capture

All original application credit belongs to Dispatcharr's authors and contributors. Existing AGPL licensing applies. This inventory comes from static original-code review during a resource-measurement quiet window. The original oracle has now run successfully in separate Factory and actual HTTP fixtures (465/463 cases); see `verified-vod-rest-source-1.json` and the sanitized `source-vod-rest-1.json`. The pure Rust policy draft remains unregistered, uncompiled and unrouted. Existing native Xtream catalog/playback evidence does not cover these REST routes.

`frontend/src/api.js:3792` onward calls movie/series/unified lists, movie detail, category list, movie/series provider lists and provider-info. `frontend/src/store/useVODStore.jsx` and Movie/Series modals consume those bodies before constructing existing `/proxy/vod/movie|episode/{uuid}` playback URLs. Successful playback alone cannot make these browser pages work.

No general `/api/vod/` route registrations were found in native Store or main at this checkpoint. These missing REST routes must not be counted as migrated based on the existing XC112 or VOD media/producers evidence. Parent owns app wiring and canonical builds.

## Original route families

All registrations are in `apps/vod/api_urls.py` (DefaultRouter), mounted under `/api/vod/`. Numeric artwork paths additionally accept no trailing slash. The inventory below separates metadata, writes, artwork, and provider refresh.

| Route | Original implementation | Permissions and important behavior |
|---|---|---|
| `movies/`, `movies/{id}/` | `api_views.py:101` MovieViewSet; `serializers.py` MovieSerializer | ReadOnlyModelViewSet list/retrieve Standard. Only movies with an active account relation, distinct. Per-user literal-false movie flag hides all. Adult hidden only for nonadmin with truthy hide_adult_content. Nested logo; model fields including language, UUID, custom_properties, timestamps. |
| `series/`, `series/{id}/` | `api_views.py:408` SeriesViewSet; SeriesSerializer | Active account relation + distinct; literal-false series flag. No Movie-specific adult predicate. Nested logo and total episode_count (not only active-provider episodes). |
| `episodes/`, `episodes/{id}/` | `api_views.py:377` EpisodeViewSet; EpisodeSerializer | Series flag. Requires the episode's active account relation; does not independently require its parent series to have an active series relation. Nested complete SeriesSerializer. |
| `movies/{id}/providers/` | `api_views.py:140`; M3UMovieRelationSerializer | Explicit Authenticated action, not Standard. Still scopes parent through get_queryset/access flags/adult visibility. Active provider relations only. Nested full M3U account serializer, category and movie; derived quality_info. |
| `series/{id}/providers/` | `api_views.py:440`; M3USeriesRelationSerializer | Explicit Authenticated. Parent scope applies. Nested active accounts/category/series. |
| `series/{id}/episodes/` | `api_views.py:458`; EpisodeWithProvidersSerializer | Explicit Authenticated. Returns all episodes for the visible parent, ordered season/episode; only nested providers are filtered active. Episodes without active/no providers can remain in this action while absent from the top-level Episode list. No list pagination here. |
| `movies/{id}/provider-info/` | `api_views.py:181`; MovieProviderInfoSerializer | Explicit Authenticated action. Optional Python-int relation_id. Defaults to highest account priority then relation ID. Synchronously invokes refresh_movie_advanced_data if forced, detailed flag falsy, missing refresh time, or age>86400s. Reloads DB. Artwork rewriting and typed serializer coercions. |
| `series/{id}/provider-info/` | `api_views.py:518`; SeriesProviderInfoSerializer | Explicit Authenticated. Same provider selection. Refresh if episode/detail flags missing, forced, missing/stale last refresh; Python-int refresh_interval default24. Calls provider synchronously, reloads; groups included episodes by season; include_episodes defaults true. Exception response has its own500 text. |
| `categories/`, `categories/{id}/` | `api_views.py:751`; VODCategorySerializer | Standard list/retrieve. Unpaginated unless original settings change. Movie/series access flags filter types. Embedded category/account relation rows are not filtered active/enabled. List has lazy writes described below. |
| `all/` | `api_views.py:831`; UnifiedContentListSerializer | Standard. Separate rawSQL union with different pagination/search/order/adult semantics; do not reuse Movie/Series list defaults. Detail route uses empty Movie queryset and should be captured independently. |
| `vodlogos/`, `{id}/`, `bulk-delete/`, `cleanup/` | `api_views.py:1117`; VODLogoSerializer | ModelViewSet reads Standard, mutations Admin via shared/action permissions. Name/url validation, usage filtering, counts/item names, cleanup physical effects require their own write/cache/file gates. |
| `vodlogos/{id}/cache[/]`, `{movies,series,episodes}/{id}/image[/]` | `api_urls.py`, image_proxy.py and RawImageContentNegotiationMixin | Explicit AllowAny actions still perform presented authentication first. Binary/image/proxy/local-file behavior and unsafe-path/source URL handling are separate gates; the metadata oracle does not fetch images. |

## Filters, search, order, pagination

Movies filter name icontains, m3u_account number, category, year, year_gte/year_lte, and is_adult BooleanFilter. Series has analogous filters without is_adult. Episodes filter name, series, account, season and episode numbers. Categories filter name/type/account. Type-qualified categories split only the final `|movie` or `|series`; unrecognized suffixes and embedded pipes remain literal name text (`utils.py:parse_category_filter_value`). Related-table filter joins and distinct ordering must be captured, including mixed-type category relations rather than assumed valid-by-construction input.

Django SearchFilter handles movie/series name-description-genre and episode name-description, unlike the unified SQL branch. Movie/series allowed ordering is name/year/created_at, default name; episode default is series name, season, episode. Category ordering defaults name; actual OrderingFilter accepted-field fallback needs source captures.

Movie/series/episode VODPagination is default20, configurable page_size, max100. Invalid/negative/zero/last pages and duplicate arguments depend on DRF parser behavior and must be captured. Category lists are plain arrays. Unified `/all/` bypasses that pagination: Python int(page_size default24), int(page default1), raw LIMIT/OFFSET, count and next/previous URI construction. Its sort is LOWER(name),id and ignores the nominal OrderingFilter. It searches only lower(name) using unescaped `%`-LIKE semantics, not the list SearchFilter. It gates movie/series access flags but has no Movie hide_adult_content predicate in its raw WHERE clause. Preserve observed behavior before proposing fixes. Unified description/genre/custom_properties coerce falsy values to empty string/object; rating becomes float. Normal serializers retain nullable strings/JSON and rating as string.

## Side effects and context-sensitive projections

Category list GET creates both global Uncategorized category rows even when content access flags will later filter the returned list. For each active XC account with truthy custom_properties.enable_vod it creates missing movie/series relations, enabled according to auto_enable_new_groups_vod defaultTrue. Existing relations are not overwritten. Capture row deltas and second-call idempotency, not only the returned JSON.

Provider-info is not a cached-only endpoint. Source and native must eventually be compared against an owned loopback provider: request count/URL/headers, stale and forced refresh, returned DB updates, throttling, empty-episode guards, invalid relations and failure cleanup. The metadata oracle blocks and records refresh calls; any intercepted request is excluded from provider-refresh/status-parity acceptance. An exception caused by the blocking fixture is not an ordinary Source server failure.

Logo cache URLs include a short MD5 source-URL hash. Serializer context matters: ordinary viewset list serializers receive request context and can produce absolute URLs, whereas provider/series-episodes actions instantiate nested serializers without request context, producing relative cache paths. Provider-info explicitly passes the request. One global DTO/origin rule would change bodies. Nested M3UAccountSerializer includes account/profile/filter/expiry projections and excludes write-only passwords; it is substantially larger than the slim provider-info account DTO.

GET/list/retrieve permissions come through permissions_for_action. Explicit provider/series-episodes/provider-info actions use Authenticated and can allow a basic user denied the normal list; their parent get_queryset still applies access flags. HEAD fallback and OPTIONS/default/method_not_allowed action mappings must be captured, not inferred from the grid's permissions_for_method policy.

## Prepared oracle and next gates

`scripts/rust-vod-api-contracts/source.py` is the guarded original APIRequestFactory/actual HTTP oracle requiring BOTH POSTGRES_PASSWORD and VOD_API_DISPOSABLE_FIXTURE to equal playback-fixture-only, plus a fresh owned namespace. It seeds roles, inactive/duplicate providers, active/inactive content, nullable/Unicode/JSON fields, access flags including numeric0, logos, categories, series and episodes with/without providers. Account/profile bulk creation intentionally avoids ingestion signals; this is not account-create parity. GET category row deltas are recorded. Network, Celery and provider refresh calls are blocked and explicitly recorded. Do not publish raw fixture tokens/account metadata; retain them only in disposable artifacts.

Run this only after quiet ends in a fresh parent-approved fixture. First freeze the original tool source and capture actual statuses/body bytes/field order/SQL and row effects. Preserve unhandled Factory exceptions separately from observed outer network errors and never infer HEAD body suppression from manually rendered Factory content. Add actual original-network normal/error/HEAD proof before app attachment.

The optional `store/src/vod_api_projection.rs` draft is not registered in lib.rs, compiled, or routed. It contains only literal-false access-flag and final-pipe category-name policies directly derived from utils.py. Full model/serializer DTO projection is deliberately deferred until the actual serializer corpus establishes field ordering, scalar types and context-sensitive nested logos; no cached-only substitute for provider-info is drafted.

Later native implementation needs physical request/DB/response-memory admission before auth/cache/SQL, bounded count/pagination/metadata projection, rollback retention after logical cancel, held-body/final-frame leases, indexed/search/filter proof, actor/task shutdown, and query-count/CPU evidence on substantial catalogues. Typed errors must not silently omit rows or normalize mismatched bodies. Existing XC112/media acceptance remains unchanged.

## Executed Source capture and reproduction

The draft runner now accepts `--mode factory|http|both`. `both` creates two separate fresh disposable Compose projects, so Factory category writes cannot warm the real HTTP fixture. It snapshots tools and original git/archive/image bindings before setup, captures each corpus/worker stderr/failure and owned DB effects, and removes its own project/volumes. No native or app image build is performed.

Executed command (repeat only in an authorized functional window with a fresh output path):

```
source artifacts/rust/tooling/environment.sh
python scripts/rust-vod-api-contracts/run.py --mode both --output artifacts/rust/m5-vod-rest-source-1
```

Direct Source script execution requires both `POSTGRES_PASSWORD=playback-fixture-only` and `VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only`, plus `VOD_API_OBSERVATION=factory|http` and a fresh owned namespace. The runner supplies these only to its owned baseline container. Real HTTP uses the unmodified original server at its container-local127.0.0.1:5656, actual JWT/permissions/middleware and clock, and a five-minute fixture expiry. HEAD bodies/Content-Length are captured from that server, never inferred from Factory rendering. Server SQL is not falsely attributed to CaptureQueriesContext in the observer: HTTP records DB before/after effects; Factory records actual view SQL.

Both fixtures begin with a both-access-flags-disabled category GET and a repeat, so lazy writes and idempotency are observable even when the returned category list is empty. The cases then cover permission roles, adult visibility, literalFalse versus numeric0 access flags, active/inactive and duplicate provider relations, name/year/account/category filters and invalid grammar, search wildcards/multi-term syntax, stable/default/invalid ordering, page/page_size/duplicates/last/limits, read-only verbs, detail filtering, basic-user explicit actions, and providerless episodes in top-level versus series action paths.

Provider relations are seeded with real fresh timestamps and fetched flags. Factory-only forced refresh calls remain blocked and recorded; those artificial errors are not provider-refresh parity. Real HTTP omits forced/stale refresh requests. Every owned provider URL points to a bounded container-local503 trap rather than a public hostname; any contact fails real-HTTP capture, and refreshed metadata/timestamp rows must remain unchanged. This proves a metadata-only/no-remote-provider capture boundary; it does not establish provider refresh behavior. Logo URLs are serialized but image endpoints are excluded. No existing accepted tools or native modules are overwritten by this draft.

`source.py`/`run.py` were executed after quiet release. Both original fixtures passed and were removed. The successful tool bytes are retained under each mode’s `tools/` directory; their initial draft comments are historical, and this execution evidence establishes their current status. The pure native policy draft remains unregistered; no native REST acceptance follows from these Source observations.


## Recorded Source result (Source-only1)

`artifacts/rust/m5-vod-rest-source-1` passed465 Factory cases and463 actual HTTP cases. Both fixtures recorded zero loopback provider-trap contacts and unchanged provider detail/refresh rows. Each recorded20 HEAD cases; Factory rendered bodies and actual HTTP wire bodies remain separate observations. Actual HTTP included256 status200,68 status403,51 status401,51 status404,20 status405,10 status400 and7 status500 responses. These are captured behavior, not a claim that every response is desirable.

Both fresh fixtures began with a both-access-flags-disabled category request. It created two global Uncategorized rows and four owned account/category links (owned relations9→13), then the repeat was idempotent. This side effect is part of the browser endpoint contract even when the filtered returned list is empty.

The two extra Factory cases deliberately forced provider refresh while the oracle blocked providers/tasks. The movie exception escaped the Factory view; the series view returned its500 wrapper. Both are explicitly excluded from metadata/provider-refresh acceptance. The seven unmodified actual HTTP500 responses were unified-list page=last/0/-1/bad, page_size=-1/bad, and series provider-info refresh_interval=bad. Future native error policy must retain these witnesses rather than treating the blocked Factory errors as ordinary server behavior or silently interpreting all malformed arguments the same way.

The sanitized dataset removes Authorization inputs and raw SQL, deduplicates complete response/effect snapshots, and records case roles/methods/paths, SQL counts, blocked-operation exclusions, source hashes and original/private evidence bindings. Raw tokens and SQL stay in private artifacts. Generated fixture IDs, clocks, metadata URLs and timestamps remain exact observations; any future native comparison normalization needs an explicit rule. No native DTO, REST route, stale-provider refresh, artwork operation, resource bound or performance result has been accepted by this capture.
