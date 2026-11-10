# Central cache and authentication scope work

All original application/algorithm credit remains with the Dispatcharr authors and contributors. Existing AGPL licensing is preserved.

This code was drafted during GRID quiet1 and executed after release. Frozen4 passed strict Store all-target Clippy and five real Redis tests (caller abort, logical timeout/composed scopes, cancelled16-command burst,256-waiter bound, WRONGTYPE/ACL error classes). The independent original Django cache interoperability gate passed values and TTLs. FrozenVOD8 passed exact201 ordinary Source/nativeSQL comparisons with real cached AuthService and six SQL/Redis/resource tests; its cached2s fallback held54,535,168B/request admission until the actual Redis reply, then9,216B for the body frame and finally zero. Exact frozen8 strict Store lint passed. All owned containers were removed. Durable summaries are `verified-cache-scope-native-1.json` and `verified-vod-rest-native-sql-3.json`.

Frozen1's initial test lint failure and frozen3's4s observer timeout against actual4.07s Redis pause remain preserved; the latter was fixed only by increasing the drain observer to6s. Runtime deadlines are unchanged. These original component gates retain their narrower scope. Later nativeHTTP5 and packagedimage31 ordinary routes have separate bindings and do not prove fullVOD/browser/performance parity. Accepted earlierVOD6/7 artifacts retain their original bytes.

Static review found NotificationCache.command wrapped a multiplexed query in a two-second timeout while holding a borrowed semaphore permit. Cancellation/timeouts dropped the observer and permit, even though the Redis driver could still have a pending response. A VOD-local wrapper that merely awaited this method would not retain request admission through that physical work.

The central implementation gives commands an owned physical task, owned semaphore lease and optional composed caller scope. Its logical two-second timeout detaches the observer; the task retains all leases until the actual reply or connection error. Sixteen operation slots remain shared by every clone (each sequential User-Agent invalidation may retain two physical command futures under one slot), and the new256-waiter guard fails with the existing unavailable classification rather than admitting an unbounded queue. This cap is an intentional bounded-admission behavior under overload, not a claim of identical overloaded timing.

General command errors keep the existing unavailable503 mapping. Default User-Agent invalidation retains its original backend/timeout ignored versus server-error500 distinction and sequential logical attempts. Both physical invalidation commands share one admitted operation slot through Arc; if one reply is late, it cannot release that slot while the other command remains pending. Payload encoding, cache keys, TTLs and generation rules are unchanged.

AuthService.with_retained_scope operates on a per-request clone, retaining the scope even without a cache or sink. It composes existing scopes, wraps the shared cache clone and delegates event publication through send_retained. Service attachment after scoping inherits that scope. The app singleton must remain unscoped. The actual M7 bus already has a retained physical-publication interface; arbitrary sinks must honor that interface rather than spawn unguarded work. No authentication, network, throttle, cache-content or credential-selection policy is changed by the bridge.

The VOD ScopedUiAuth/from_auth seam is validated and used by the public ordinary read constructor. It uses actual cached AuthService on a request clone, not fixed users or a PG-only bypass. NativeHTTP5 passed393 captured representations, simultaneous10 and physical ownership gates; packaged31 passed the same393 under app security/CORS, simultaneous10, unchanged metadata/provider rows and gracefulexit0. See `verified-vod-rest-native-http-1.json` and `verified-vod-rest-packaged-image31-1.json` for separate immutable scopes.

Completed validation:

- Exact frozen Store all-target strict Clippy and fresh current-workspace helper validation passed; failures are retained. Generic owned results now require Send/static.
- Five actual isolated Redis tests passed: caller abort, two-second logical timeout/composed scopes, cancelled16-operation burst,256-waiter capacity/cancellation, and WRONGTYPE/ACL generic503 versus User-Agent server500.
- Actual cached UI authorization→PG fallback retained its VOD worker/work memory through the physical Redis reply. HTTP handler abort likewise retained HTTP and VOD admission. Detached success/error frames retained independent finalclone ownership.
- Actual original Django/Rust booleans/dicts/group fallback and TTL interoperability passed. Original ordinary201 SQL, selected/fresh metadata schedule and category committed-prefix helper regressions passed. Packaged wire acceptance does not replace these physical tests.
- `cache_scope.py --output <unique-path>` now freezes current `ROOT/rust` by default, pruning target/.git/__pycache__. `--frozen-rust-source` selects an explicit coherent input. The fresh-source execution passed and removed its owned Redis container; no historical private artifacts are required.

Remaining distinct gates:

- Broader affected auth/cache/default-UA packaged suites are parent-owned. Low-level error-class tests do not prove every endpoint policy or overload timing.
- Additional backend disconnect/auth, cancelled User-Agent invalidation and late sequential reply schedules beyond the existing generic/ACL witnesses remain explicit.
- Auth lockout inbox/publication scope under delayed owned M7 Redis sends needs its own end-to-end schedule; the retained bus interface already exists, but no new producer is inferred.
- Full VOD category/unified/provider/action/artwork/browser behavior and allocator-wide amplification are separate from these ordinary read scopes. No performance result follows from functional ownership tests.

No tests ran during quiet. Parent-owned app attachment occurred only after the scoped HTTP/resource gates; historical frozen4/8 evidence and its tools remain unchanged.
