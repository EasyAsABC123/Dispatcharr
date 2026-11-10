# Central cache and authentication scope work

All original application/algorithm credit remains with the Dispatcharr authors and contributors. Existing AGPL licensing is preserved.

This code was drafted during GRID quiet1 and executed after release. Frozen4 passed strict Store all-target Clippy and five real Redis tests (caller abort, logical timeout/composed scopes, cancelled16-command burst,256-waiter bound, WRONGTYPE/ACL error classes). The independent original Django cache interoperability gate passed values and TTLs. FrozenVOD8 passed exact201 ordinary Source/nativeSQL comparisons with real cached AuthService and six SQL/Redis/resource tests; its cached2s fallback held54,535,168B/request admission until the actual Redis reply, then9,216B for the body frame and finally zero. Exact frozen8 strict Store lint passed. All owned containers were removed. Durable summaries are `verified-cache-scope-native-1.json` and `verified-vod-rest-native-sql-3.json`.

Frozen1's initial test lint failure and frozen3's4s observer timeout against actual4.07s Redis pause remain preserved; the latter was fixed only by increasing the drain observer to6s. Runtime deadlines are unchanged. Neither these component gates nor the source-only553 HTTP characterization register production routes or prove fullVOD/browser/performance parity. Accepted earlierVOD6/7 artifacts retain their original bytes.

Static review found NotificationCache.command wrapped a multiplexed query in a two-second timeout while holding a borrowed semaphore permit. Cancellation/timeouts dropped the observer and permit, even though the Redis driver could still have a pending response. A VOD-local wrapper that merely awaited this method would not retain request admission through that physical work.

The central implementation gives commands an owned physical task, owned semaphore lease and optional composed caller scope. Its logical two-second timeout detaches the observer; the task retains all leases until the actual reply or connection error. Sixteen operation slots remain shared by every clone (each sequential User-Agent invalidation may retain two physical command futures under one slot), and the new256-waiter guard fails with the existing unavailable classification rather than admitting an unbounded queue. This cap is an intentional bounded-admission behavior under overload, not a claim of identical overloaded timing.

General command errors keep the existing unavailable503 mapping. Default User-Agent invalidation retains its original backend/timeout ignored versus server-error500 distinction and sequential logical attempts. Both physical invalidation commands share one admitted operation slot through Arc; if one reply is late, it cannot release that slot while the other command remains pending. Payload encoding, cache keys, TTLs and generation rules are unchanged.

AuthService.with_retained_scope operates on a per-request clone, retaining the scope even without a cache or sink. It composes existing scopes, wraps the shared cache clone and delegates event publication through send_retained. Service attachment after scoping inherits that scope. The app singleton must remain unscoped. The actual M7 bus already has a retained physical-publication interface; arbitrary sinks must honor that interface rather than spawn unguarded work. No authentication, network, throttle, cache-content or credential-selection policy is changed by the bridge.

The VOD ScopedUiAuth/from_auth seam is tested but remains unregistered. It uses the real cached AuthService on a request clone rather than a fixed user or PG-only policy bypass. Independent nativeHTTP5 has since passed its captured methods/headers/burst and ownership gates. Parent-owned canonical app/package regressions remain separate before shipped acceptance.

Remaining validation:

- Compile and strict-lint all affected callers, preserving first errors. Generic result types now need Send/static to enter owned tasks.
- Actual disposable Redis: abort a pending command, exceed its logical timeout, compose two guard owners, and cancel a16-command burst. Physical slots and scopes must survive until actual replies and return exactly once. Prove256-waiter capacity and cancellation cleanup rather than merely checking normal commands.
- Actual auth/UI request: pause cached network/config lookup and cancel/timeout while a scoped read is active. A completed SQL fallback/error must not release the VOD slot while its cache reply remains pending. Keep independent response-frame capacity after work drains.
- Source-compatible cache/type/codec/TTL/generation fallback and default-UA I/O/server classification regressions. Deliberately exercise wrong types, backend disconnect/auth/ACL errors, cancelled invalidation and late sequential replies; normal API corpora alone are insufficient.
- Lockout inbox/publication: retain request scope through owned M7 Redis sends after an auth error, preserving the original envelope, cooldown, SQL effects and delivery behavior. No unsolicited event producer is added.
- Re-run VOD201/physical3/metadata schedule, source headers/roles and full affected API/cache suites, then actual nativeHTTP/React scope and a canonical packaged build. Current data/Source equality does not establish these new shared physical guarantees.

The Redis tests were not run during quiet; subsequent executed artifacts are listed above. No new app/main/Cargo/production router registration accompanies this draft.
