# Ordinary VOD reads: implementation and remaining gates

All application and algorithm credit remains with the original Dispatcharr authors and contributors. Existing AGPL licensing is preserved.

Image31 implements the explicit trailing-slash Movie, Series and Episode list/detail routes. Its actual packaged app matched393 original network responses, including status, exact body hash, MIME, Content-Length, Allow, Vary, Bearer challenge and the four Django security headers. Original and native simultaneous10 reads returned the same successful body without staggering. Owned category/metadata/provider-refresh rows remained unchanged; the app exited0 during graceful shutdown and the disposable Compose project was removed. See `verified-vod-rest-packaged-image31-1.json` for immutable image/source/tool bindings. These are functional results, not performance or wholeVOD/browser acceptance.

The public API is `VodReads::new(auth.clone(), VodReadLimits::default())`, `.router()` and `.shutdown(Duration)`. Default limits remain two active reads and192MiB configured memory. A bounded256-waiter queue charges input against the memory left after active reservations, with a five-second arrival deadline. Its necessity was established by a retained failure: original simultaneous10 all200 versus the earlier native2success/8capacity503. Bounded waiting fixed that discrepancy without increasing active workers or memory budgets.

## Route scope

| Surface | Current status | Remaining work |
|---|---|---|
| `/api/vod/movies/`, `/api/vod/series/`, `/api/vod/episodes/` | Packaged ordinary GET/HEAD, admin OPTIONS and permission-before405 matched captured network cases; actual shared cached UI auth | Raw forms/negotiation outside the captured subset; full unchangedReact workflow |
| Same prefixes `/{pk}/` | Packaged ordinary details, captured invalid/overflow PK responses, HEAD and detail OPTIONS matched; missingPK OPTIONS remains metadata200 for admin | Additional Unicode/encoded-PK, suffix and error-precedence combinations |
| `/api/vod/categories/` and `/{pk}/` | No ordinary-adapter route. Low-level lazy-seed helper has actual Source/PG normal, idempotence, committed-prefix failure and retained-owner evidence | Full category SQL/serializer/filter/order, lazy writes through HTTP/HEAD, concurrency and packaged/browser proof |
| `/api/vod/all/` | No ordinary-adapter route. Source/pure DTO/pagination policies captured | Source rawSQL union/page-before-count,24 default/no100 cap, errors and exact JSON/URL/order; actual SQL/HTTP/package proof |
| Movie/series `/{pk}/providers/`, series `/{pk}/episodes/` | Not ordinary read routes | Basic authenticated permission, parent visibility, nested provider/account context and providerless episodes; actual SQL/HTTP/package proof |
| `/{pk}/provider-info/` | Provider refresh capability remains unmigrated; no cached-only or empty-success substitute | Fresh/stale/forced on-demand refresh, network/task/DB effects, rollback and provider trap gates |
| `vodlogos`, artwork, image/cache aliases | Separate capabilities, not a VOD catchall | Binary/local/remote/cache/CRUD contracts, presented-credential authentication even under AllowAny |

There is one shared ordinary runtime, not a new budget per request. No wildcard VOD success fallback or React replacement hides unimplemented APIs. Suffix/no-slash requests are not new ordinary-adapter capabilities; existing route precedence is preserved. In particular, original no-slash `/api/vod/movies` reaches its existing XC path, rather than providing an ordinary collection alias.

## Authentication and ownership

The retained cached AuthService bridge is validated. It uses a per-request clone of actual UI authentication and network policy, not a fixed user, STREAMS/XC_API policy or PG-only cache bypass. Shared NotificationCache tasks retain owned admission and composed request scopes through replies after logical timeout/caller cancellation. The actual delayed-Redis→PG-fallback test retained its VOD worker and work memory until the real Redis reply, then retained only the detached response frame, then released all memory. Wrongtype/ACL error classification, bounded16-operation/256-waiter admission and original Django value/TTL interoperability have separate recorded gates in `verified-cache-scope-native-1.json` and `verified-cache-scope-native-2.json`.

Independent HTTP5 additionally proved held401 error-frame/finalclone admission, cancelled/expired queued input cleanup, queued shutdown without auth/SQL, and cancelled cached authorization retaining both HTTP and VOD slots until the physical reply. SQL lock/abort/deadline/rollback and selected-logo-scalar versus fresh nested-metadata schedules have actual PG/Source evidence. These component ownership witnesses are distinct from packaged wire equality. Allocator-wide amplification and arbitrary metadata complexity beyond declared row/document limits are not inferred.

Admission precedes authentication/cache/SQL. The owned actor preserves negotiation before actual authentication, action permission/UI policy, visibility, captured filter/search/order, count/page, detail lookup and metadata projection. Disabled access flags still validate captured bad filters/pages. Unsupported methods authenticate/authorize before405. Request bodies are dropped without eager JSON parsing; these readonly methods do not consume `request.data`.

Shared Latin1 canonicalization, duplicate-header precedence, scoped-IP behavior and raw malformed Authorization/API-key rejection remain in the existing auth layer. The ordinary adapter does not replace these with UTF8-only header filtering. Additional malformed-header combinations remain distinct from the valid-role corpus rather than being relabeled proven.

## Representations and headers

GET and HEAD perform the same actual ordinary read. HEAD suppresses both successful and error bodies while retaining the representation Content-Length. The captured fixture's Movie list8512-byte length is data-dependent, not a handler constant.

Image31 matched `application/json`, `Allow: GET, HEAD, OPTIONS`, `Vary: Accept, origin`, and the Bearer challenge across captured success/error methods. It also matched shared app `nosniff`, frameDENY, referrer and coop headers. Plain admin OPTIONS supplies the original ordered name/description/renders/parses metadata even for missing detailPK; lower roles fail before metadata. This is distinct from CORS preflight middleware behavior.

Captured wildcard/q/unknownformat/406 behavior and indentation4,0,negative,8,99,invalid,quoted4—including error/HEAD representations—matched. Clampedzero means compact JSON in original DRF, not Python's newline-producing `json.dumps(indent=0)`. Formatting scans already valid JSON into a bounded document without constructing a second Value tree. Indent/capacity/string-escaping unit coverage complements actual network bodies. Browsable HTML remains explicitly501; advertising the original renderer names in OPTIONS does not claim an HTML template implementation. Media-negotiation grammar beyond captured cases remains open.

## Remaining validation and browser work

| Area | Completed scope | Still open |
|---|---|---|
| Ordinary data | Original928 metadata observations (Factory/network distinguished), cachedAuth SQL201, nativeHTTP393 and packaged31 same393 | Broader raw query grammar, concurrent mutations beyond captured metadata schedule, additional PK/suffix forms |
| Methods/headers | Captured GET/HEAD/OPTIONS/unsupported verbs, roles, JSON indent, MIME/length/challenge/security headers | Malformed-body precedence combinations and specific CORS preflight combinations |
| Resources | Actual PG/Redis cancellation/deadline/rollback/frame ownership; bounded waiters and unstaggered10; packaged gracefulexit/cleanup | Whole allocator/process resource characterization and newly authorized per-milestone performance experiment |
| Category/unified/actions | Actual Source corpus and low-level category autocommit helper | Their actual native SQL/HTTP/package/browser implementation |
| Browser | React is preserved | Complete category/unified/provider/artwork-dependent VOD page and modal workflow; generated UUID playback is separately gated media functionality |

React `api.js:3793` onward calls lists, unifiedAll, categories, details, providers and provider-info. `useVODStore.jsx` uses unifiedAll for the defaultAll branch and modals consume provider data. The ordinary routes therefore do not complete the browser page. Do not modify React to conceal missing responses or label excluded forced-refresh interceptions as provider-refresh parity.

No tests ran during the GRID quiet window. All later tests have their own frozen provenance. Neither the captured functional results nor the implementation registration claims performance gains or a complete migration milestone.
