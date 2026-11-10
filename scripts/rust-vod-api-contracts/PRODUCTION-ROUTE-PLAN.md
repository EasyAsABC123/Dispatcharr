# VOD production read route plan

All application/algorithm credit remains with the original Dispatcharr authors and contributors. Existing AGPL licensing is preserved. The plan began during GRID quiet1. After release, shared cache4 passed physical supervision/queue/ACL tests and original Django value/TTL interoperability. VOD8 passed201 ordinary SQL comparisons with real cached AuthService and six physical/schedule/category tests. Independent nativeHTTP5 now passed393 original-network representations, actual simultaneous10 reads, all eight real fixture tests, strict frozen Store lint and the bounded formatter unit. These are recorded in `verified-vod-rest-native-http-1.json`; no performance or wholeVOD acceptance is inferred.

Minimal ordinary list/detail routes are now registered as a public Store constructor/router component, with parent-owned app attachment/package gates next. `VodReads::new(auth.clone(), VodReadLimits::default())`, `.router()` and `.shutdown(Duration)` preserve two active reads and192MiB configured memory. The bounded256-waiter queue reserves input from the memory left after active reservations and applies the same five-second arrival deadline. Its necessity was measured: Source simultaneous10 all200 versus pre-fix native2success/8capacity503. The fixed gate passed all ten without staggering or increased budgets.

HTML remains explicitly unsupported; categories/unified/provider/actions/artwork and source suffix/no-slash behavior are not enabled by this ordinary adapter. Original928 Factory/network metadata evidence retains its separate scope, including the two excluded forced-refresh interceptions. The updated actual-network corpus contains592 observations, of which393 belong to the adapter's captured routes/methods/representations. No canonical image/browser workflow or allocator-wide memory guarantee follows from these component results.

## Explicit routes and capability boundaries

| Surface | Initial production candidate | Source policy that must precede handler behavior | Remaining work |
|---|---|---|---|
| `/api/vod/movies/`, `/api/vod/series/`, `/api/vod/episodes/` | Ordinary bounded list reader | GET/HEAD Standard + UI policy; literalFalse access gates and Movie-only nonadmin adult predicate; validate filters even for empty-access querysets | Real HTTP/full methods/headers, expanded raw forms, browser/package gates |
| Same prefixes `/{pk}/` | Ordinary bounded detail reader | Same auth/visibility/filter pipeline; convert/lookup PK after earlier errors, not pre-auth404 | Raw PK/suffix/slash grammar and detail metadata OPTIONS capture |
| `/api/vod/categories/` and `/{pk}/` | Separate category reader, not covered by ordinary component | Standard; list performs lazy writes before visibility/filtering, including disabled flags; HEAD executes same list handler | Serialize/filter/order unpaginated categories, retained write owner, method/header/package proof; low-level helper alone is insufficient |
| `/api/vod/all/` | Separate unified reader | Standard;24 default and rawSQL/Python-int errors; LOWER(name),id; no Movie adult predicate; no ordinary100 cap | Full union/count/URL/error/byte behavior and HTTP proof; do not substitute concatenated movie/series pages |
| Movie/series `/{pk}/providers/`, series `/{pk}/episodes/` | Separate explicit action readers | Authenticated permits Basic users; parent visibility still applies; nested provider data/context differ | Exact nested account/relation DTOs and providerless episodes; do not accidentally demand Standard because ordinary details do |
| `/{pk}/provider-info/` | Explicit unsupported refresh capability until its own gate | Presented auth/UI policy and parent visibility first; fresh/stale/forced provider semantics are not a generic read cache | Fresh projection and actual provider refresh/effects/rollback/trap gates; never emit cached-only/empty success for a refresh that Source performs |
| `vodlogos`, artwork and no-slash image/cache aliases | Separate capabilities, not a catchall | Artwork AllowAny still authenticates presented credentials; generic artwork CRUD/admin rules differ | Binary/proxy/local-file/CRUD/cache gates remain independent |

A future app constructor should create one shared VOD runtime and clone it into explicit route handlers. No per-request runtime/budget constructors, wildcard `/api/vod/*` success fallback, React asset fallback for unknown API paths, or fake authenticated users. Expose only the constructor/config/shutdown interface once all necessary integration gates pass; Source datasets/test peers stay test-only. Parent owns app/main and canonical builds.

## Request order and ownership

Acquire bounded request/work/response admission before auth/cache/SQL. Inside the owned actor, preserve DRF order: content negotiation, actual shared authentication, action permission/UI network policy, get_queryset visibility, form validation/filter/search/order, count/page, detail lookup, metadata projection. Disabled flags do not bypass invalid form/page errors. Use UI, not STREAMS or XC_API, for these REST permissions.

The retained cached AuthService bridge is prerequisite and currently unvalidated. A per-request clone must carry the guard into supervised physical cache commands and M7 publication. Its normal/error return cannot release admission while an internal timed-out Redis reply is still pending. No cache bypass/fixed user may be accepted as production authentication. Existing Latin1 header canonicalization, folded duplicate precedence, scoped-IP handling and raw malformed Authorization/API-key rejection remain shared contracts; do not replace them with generic HeaderValue.to_str filtering.

Receive method/URI/headers and bounded raw body rather than an eager Json extractor. Read-only unsupported verbs do not access request.data; an invalid JSON body must not mask an earlier401/403 or Source405. Raw path conversion must likewise not precede auth/filter errors. Content-Type parser rejection is lazy in Source and is not equivalent to Accept renderer negotiation.

GET/HEAD data paths share the same actual work and side effects. Build the success or error representation, preserve its exact byte length and headers, then suppress the HEAD wire body and release its frame owner. A HEAD shortcut that skips category lazy writes, returns Content-Length0, or authorizes differently is wrong. Default Axum `get()` method rejection before route auth is insufficient: explicit method dispatch must retain the Source permission-before405 behavior.

## Captured wire facts and header ownership

Source1 actual HTTP `/api/vod/movies/` standard GET returned200 with8512 bytes in that fixture; HEAD returned200 with zero wire bytes and Content-Length8512. Basic HEAD returned403, zero wire bytes and Content-Length63. These are fixture-specific lengths, not constants to bake into handlers.

The same captures show `Content-Type: application/json`, `Allow: GET, HEAD, OPTIONS`, and `Vary: Accept, origin` across successful/error method responses. Anonymous POST returned401 with `WWW-Authenticate: Bearer realm="api"`; Standard POST403; Admin POST405. Shared StoreError already adds the Bearer challenge; the VOD HTTP assembler must add Allow/Vary on those errors and correctly suppress HEAD error bodies too. Preserve/merge existing Vary rather than replacing origin.

Reuse the app's accepted shared security/CORS layer: nosniff, frameDENY, referrer/coopsame-origin and origin variation. Validate under that actual layer after packaging. Do not duplicate or silently omit those headers in an isolated peer. CORS preflight middleware is a separate path: an actual Origin/Access-Control-Request-Method preflight is not the plain viewset OPTIONS case described below.

Admin plain OPTIONS on the captured collection routes returned200 metadata with ordered `name,description,renders,parses`. Standard OPTIONS403 and anonymous401 follow action-default Admin. Ordinary Admin POST/PUT/PATCH/DELETE405 and lower roles denied before405. Original Movie metadata is `Movie List`, description `ViewSet for Movie content`; Series is `Series List`/`ViewSet for Series management`; Episode is `Episode List`/`ViewSet for Episode content`. Original renders are json/html and parsers json/form/multipart. Do not invent empty OPTIONS200 or claim HTML implemented from advertising Source metadata. Native browsable HTML remains an explicit renderer gate.

DefaultRouter suffix/optional-slash behavior, collection noslash redirects, malformed/Unicode/overflow PKs, unknown format, detail/action OPTIONS metadata, Accept q/wildcard grammar and CORS preflights need additional original-network cases. Existing Source1 uses clean trailing-slash paths and application/json. Do not generalize the201 GET proof to those unobserved routing cases.

## Concrete post-quiet tests

| Matrix | Original/native observation and required result |
|---|---|
| Implemented GET baseline | Replay201 supported ordinary GETs through an independent native HTTP peer and actual original HTTP on the same fixture; status/body hash/MIME/Content-Length/Allow/Vary/challenge exact; count unsupported families separately |
| Roles and failure precedence | Admin/Standard/Basic/inactive/anonymous, literalFalse/numeric0/adult flags; invalid JWT/API-key, duplicated/mixed-byte auth, malformed forwarded ACL/scoped-IP; invalid filters/PK/body must not override earlier auth/permission errors |
| HEAD | Success,401/403/404/400/capacity error on list/detail; zero body but original representation length; same auth/filter behavior; category side effects/order and no phantom view/client metadata |
| Methods and OPTIONS | Full six-method Source1 cases plus detail/action plain OPTIONS, invalid PK/options and malformed JSON/body Content-Type; metadata ordered fields exact; shared CORS preflight tested separately |
| Negotiation and URL routing | Valid/unknown `.json`/format query, no-slash forms, unicode/bad/overflow PK, JSON/wildcards/q-values/HTML/unsupported MIME; capture Source first and preserve explicit unported HTML capability |
| URL/context | Trusted/untrusted forwarding, port/scheme/origin, percent/duplicate query rewriting; absolute list/detail logo URLs versus relative action nested URLs; no untrusted header privilege changes |
| Physical ownership | Repeat realPG lock/abort/deadline/finalclone and category pending insert/failure with actual nativeHTTP disconnects; pause actual Redis during cached authorization/publication; slots remain until real reply/rollback; capacity never becomes a successful empty/truncated response |
| Category/read state | Exact list/detail JSON and lazy0→2 global/9→13 owned effects, defaults/idempotence, concurrent get_or_create and injected failure prefix; row IDs normalized only where explicit fixture reset creates new semantic entities |
| Source snapshots/order | Same fixture EXPLAIN shows Source-shaped page/CTEScan with no new sort/window/join; retained page ties exact; selected logo scalars versus fresh counts/names/episode-count interleavings; large-page metadata guards and query counts |
| App/browser/package | Actual immutable app security/CORS/router/shutdown; unchanged React bundle and real user accounts, category fetch, Movie/Series pagination/search/detail, unifiedAll and provider-driven modals, generated UUID playback/decode; no fake fetch/JSON replacement |

React `api.js:3793` onward calls lists, unifiedAll, categories, details, providers and provider-info. `useVODStore.jsx` branches to unifiedAll when type is neither movies nor series, and modals consume provider data. Therefore a three-route ordinary prototype is useful incremental scope but does not complete the VOD browser workflow. Do not modify React to conceal missing responses. Complete or explicitly gate remaining surfaces before declaring whole-page/milestone parity; provider refresh501/traps must stay visible rather than being labeled passing.

No validation ran during GRID quiet. Subsequent Source/resource bindings are listed at the top; canonical-image, shared security/CORS and unchanged React acceptance remain next.
