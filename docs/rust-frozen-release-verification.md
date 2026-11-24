# Dispatcharr Rust release implementation and verification

The last reconciled package, `2d3ffe53`, has 14 verified executions, four retained Original authorities, and 109 open rows in the 127-row release matrix. Each open row requires a concrete implementation result, a complete executable owner and evaluator, a prerequisite result, or a release decision. The matrix identifies every row and its outstanding predecessors. [Current release matrix](../scripts/rust-integration/2d3ffe53-amd64-release-matrix-with-baseline.json)

## Parallel implementation

| Canonical owner | Open rows | Work before the freeze |
| --- | ---: | --- |
| Root, M0, M1, M9 | 7 | Complete the pinned Original executable HTTP and non-HTTP inventory, establish supported datastore upgrade scope, and prepare final core, workflow, soak, rollback and whole-service performance owners. |
| M235, M2/M3/M5 and packaged workflows | 27 | Assemble the remaining current package, proxy, profile, accounts, settings and React owners. Bind real lifecycle faults, compiled peers and signed Main effects using the established dependency order. |
| M478, M4/M7/M8 | 27 | Complete output, VOD, plugin/operator, backup and upgrade workflows. Bind complete signed backup operations and the separately verified callback/job lifecycle. Preserve exact Original evidence where the owner admits it. |
| M6 | 48 | Finish native ingestion, worker, scheduler, calendar and failure/recovery owners. The two partial probes and missing full owner adapters require real typed business and resource predicates before they can satisfy release rows. |

A lane records the affected row, the evidence for a product gap, and the exact prepared runner and evaluator. Existing Original observations remain bound to their verified Original revision and selectors. Rust behavior receives release credit from the frozen current package. A failed earlier receipt stays attached to its original fixture or package. When behavior is already present, the lane completes the executable owner and independent evaluator for the required scope.

## Provider playback implementation

Grouped XC catch-up now derives the Original-compatible shared credential counter from the selected transformed login. Limited profiles use it alongside their profile counters during archive reservations, reopens and the read-only Redirect capacity check. Unlimited profiles bypass both counters. Grouped STD live playback accepts a sample with a plain query in the qualified format and preserves the same counter when the normal route selects FFmpeg or a configured input command. Ordinary grouped STD Redirect also carries this counter for both assigned stream selection and requests with a provider profile allowlist.

Grouped STD credential selection now checks typed live, movie, and series paths before stored account credentials, then checks short-form URLs. It decodes escaped short-form credentials once and preserves the raw typed-path spelling, matching Original’s two parsers. Ordinary media paths can use the stored login. A profile’s qualified literal or anchored rewrite applies to both the selected media URL and the pool sample. The final paired playback fixtures still need to verify live counters and release.

The catch-up and STD units passed, and the signed PostgreSQL resolver and real-Redis test targets compiled. The final paired workload will exercise shared capacity and release under playback, distinct profiles with the same login, an idle cross-worker reopen, and the direct and command-based STD formats. The existing preview, redirect, and transformed credential domain limits stay in the test plan.

## Channel Batch and VOD routes

Administrator Channel Batch now accepts ordered stream replacement and explicit clearing. Stream references are validated across the request before writing, surviving links retain their IDs, and the existing catch-up rollup updates the persisted channel. PATCH preserves the previously loaded catch-up values, which a fresh GET then reads from the updated channel.

Channel Batch also accepts raw channel numbers, TVG IDs, guide station IDs, and EPG assignments. It distinguishes explicit clears from omitted fields and preserves the existing compact-number behavior when a request also changes visibility. Raw EPG writes preserve Original's bulk-update behavior and skip guide publication.

Nested Channel Batch overrides support name, TVG ID, guide station ID, channel number, and EPG assignments. Guide changes invalidate the cache once and publish distinct programme jobs. For Schedules Direct, the plan skips guides with existing programmes, publishes individual jobs below three unfilled mappings per source, and publishes one mapped-guide job at the threshold. The normal application binds the publisher to its registered Schedules Direct worker.

Numeric Movie, Series, Episode, Category, and Unified detail GETs and Movie, Series, Episode, Category, and Unified collection GETs accept Original's literal JSON suffix. M3U Accounts, EPG Sources, and Core Settings also accept that suffix on collection and numeric detail GETs. Numeric Notification detail GETs use the same per-user visibility rules and response serializer on the JSON suffix routes. Accounts Group detail and recurring DVR rule collection and detail GETs also accept the suffix through their existing access checks. The Connect integrations collection accepts the same suffix on its bounded administrator read. The aliases reuse existing authorization, SQL reads, negotiation, and credential filtering. Focused Store tests and the ordinary application build check passed. The final Compose fixtures still need to verify signed roles, persistence, and real guide job settlement.

The unsigned plugin repository owner can schedule the full 25-call comparison against the frozen candidate using the retained 24 Original observations and one retained 404. It independently checks application and fixture retirement, and 11 offline fixture controls passed. Full plugin lifecycle and performance retain their own release checks.

## HDHomeRun HTTP output profiles

HDHomeRun discovery, lineup, and status paths now accept percent-encoded decimal output-profile IDs. The router still requires ASCII digits after decoding and canonicalizes leading zeros, including on named ChannelProfile paths. Fifteen HDHomeRun library tests passed. The configured-client Compose fixture will check the ordinary signed package response and encoded aliases.

Registered public HDHomeRun data routes also accept the Original browser format on GET and HEAD, including named output profiles and the bare API aliases. Focused tests exercise content negotiation and preserve the existing authorization and device-data paths. The paired ARM64 owner will verify these requests on the final package.

M3U, XMLTV, and HDHomeRun also share Origin URL formatting for trusted proxy headers. Empty optional forwarding headers fall back to the normal host or port. The existing per-request proxy trust check supplies that decision. Both output crates compiled and the shared formatter matched Original across 2,520 header combinations.

M3U, named XMLTV, and HDHomeRun now share the Original channel-number formatter. It preserves the fractional digits for binary64 ties in both displayed channel numbers and generated guide IDs. Focused Store, output, and all 15 HDHomeRun tests passed, and a separate comparison matched the pinned Python formatter across 49,990 finite inputs.

Xtream playlist and XMLTV requests with the single trailing slash now reach the same authenticated routes as `/get.php` and `/xmltv.php`. Both output crates compiled. The final signed fixture will compare the slash forms and their response bodies.

## First-admin setup on a clean install

The ordinary PostgreSQL service now emits the same ordered JSON setup documents as pinned Original. It shares the formatter with the copied SQLite runtime. The existing clean-install controller checks the exact status, media type and response bytes before the first administrator is created and after restart. It records a digest and length for each response. The focused PostgreSQL formatter, three copied SQLite setup tests and two controller checks passed.

## SQLite backup creation and restore

The backup library now selects its snapshot from the admitted runtime datastore and writes both formats with the same bounded archive writer. PostgreSQL keeps the existing dump path. The optional SQLite path uses SQLite’s online backup API so the archive includes committed WAL data. Its version 2 archive contains `database.sqlite3` and `metadata.json`.

The optional restore path checks archive type and reads the staged SQLite catalogue before touching the target. With a quiesced datastore owner, it rejects open journal companions, retains the previous database as a rollback copy, and installs and verifies the recovered file. Linux passed 28 feature-enabled and 17 default backup tests.

The backup component uses the locked bundled SQLite library already used by the copied runtime's SQL driver. The copied runtime now answers a repeated setup POST when an administrator exists and exposes `/health/ready` with SQLite and Redis checks. On a migrated copy with no administrator, the same setup route can create the first account under the existing client IP policy. It uses the shared bootstrap parser and password worker, then writes an administrator row to the owned SQLite database. The copied-runtime Store tests passed, and the application and backup test targets linked with both optional features enabled.

The ordinary package still needs SQLite startup, ownership wiring, migration compatibility, and image linkage. The SQLite component tests alone do not close M8 or M9.

## Recording and profile delivery owners

The one-off Beat row starts one signed recording fixture and verifies HTTP creation, the clocked SQL entry, one due-time broker publication, and its durable result. The scheduled 61-second recording row consumes that same owned fixture and evaluates media, recorder retirement, and cleanup independently. The coordinator admits both bindings before physical results exist and keeps their execution order. The known-default installation owner checks three ordered SQL receipts, reinstalls the removed status schedule with zero runs, and validates the other five rows and teardown against the retained Original roster. The Native profile-delivery owner reuses the two retained Original profile authorities, and the isolated controller validates the repository holding those receipts before admitting the binding. The coordinator and recording regressions passed offline, including recording admission before any physical receipt exists.

The hourly scheduler tick uses the same owned startup as the saved-default installer. It records both deliberate SQL due arms, ties them to two broker UUIDs and the intervening artwork child, and verifies the bounded publication time and terminal cleanup. The old Native journal remains useful for replay. Canonical credit requires the new due-arm receipts. The 21 M6 owner tests and 34 coordinator tests passed offline.

The ordinary scheduler normal-stop row reuses this fixture. Its evaluator requires the actual Main stop command, exit zero without OOM, unchanged run count, empty ready and reserved queues, and exact cleanup. The combined M6 owner suite passed 23 offline tests.

The EPG import route now attaches the declared POST and OPTIONS methods and the Accept variation to handled API responses. Front-door admission, over-limit, unsupported and uncaught server responses keep their own headers. A focused job-transport test passed, and the paired signed import owner still needs the actual worker, broker and cleanup receipts.

## Core settings publication during shutdown

Selected Main drains its workers while retaining the shared Jobs broker. After the stream and HTTP drains, Main settles the already-admitted Core settings requests and then closes the broker. This lets a settings request reach its first broker lease after a held database write clears. The remaining broker drain budget still comes from the selected worker drain. The app compiled and both files passed formatting. The final regression holds a real PostgreSQL update across termination and checks for the corresponding durable publication and exact resource cleanup.

## VOD admission cleanup

When all providers definitely refuse a new VOD session at capacity, the 503 response now waits within its bound for token-fenced cleanup of the provisional session. The retained cleanup owner handles unresolved Redis replies. Four authenticated-restart tests passed and the held-Redis-reply regression compiled for the final fixture.

A retained VOD session that re-enters during its ordinary idle grace now uses the current connection limit for the same provider. A confirmed late capacity refusal returns 429 and preserves the unrelated reservation. The focused provider-identity test passed, and the Redis regression compiled. The final paired fixture also needs to capture the retained generation and useful progress on the other movie.

## Output index and Original method declarations

The ordinary backend now returns Original’s permanent redirect from `/output` to `/output/`, with an empty body and no forwarded query string. Its route-level tests passed, including safe methods and CSRF enforcement. The paired Compose output fixture will verify the real response.

`scripts/rust-baseline/runtime_dispatch_methods.py` can run inside pinned Original to supplement the existing route inventory with Django class handlers and explicit method decorators. It cross-checks the exact retained resolver ordinal, callback, and router action map before reporting declarations. Five offline controls passed. Runtime discovery, signed responses, and Rust request parity still require separate receipts.

## Accounts and Xtream compatibility aliases

The ordinary Accounts User detail route accepts the numeric JSON suffix for GET and keeps the existing administrator permission check and response shape. The signed fixture pairs it with the existing Group and recurring-recording aliases and checks that the target and scoped SQL state stay inert. Two Accounts tests passed.

The Xtream gateway also accepts one trailing slash for `/player_api.php` and `/panel_api.php`. The gateway suite passed 65 tests. The ARM64 fixture signs in as a Standard user and checks that the same live-stream request returns the expected list in the player and full account document in the panel, with the same behavior through the slash aliases.

The Core UserAgent and StreamProfile JSON collections also accept their registered trailing slash. GET and POST use the same authentication, representation and method handlers as the existing spelling. A focused test checked the exact authentication responses for both methods and both spellings. Signed collection responses and mutation effects remain part of the final packaged Core fixture.

The Channels summary and number-range JSON collection actions also accept their registered trailing slash. Both spellings use the existing query and permission handlers. The focused router test exercised 12 requests, and the same-copy Channels fixture will check signed responses on the final package.

The Logo JSON collection also accepts its registered trailing slash for GET and uses the same permission and serialization paths. The focused router check passed. The signed Channels and Logo fixture will compare 24 requests per backend against the same fixture state.

Saving User channel profiles or Group permissions keeps a singly requested existing join row without spending another PostgreSQL sequence value. The set update retains Original’s conflict-ignoring insert attempt when the request repeats a target. Five focused admin tests passed. The paired fixture will compare the row identities and sequence changes alongside signed access and channel visibility.

## EPG Programme JSON and HDHomeRun renderer selection

Both numeric EPG Programme JSON routes accept a trailing slash for GET through their existing permission checks and bounded renderer. The focused Store check covers both routes and checks the retained JSON representation behavior. The paired fixture will also check the signed Programme responses and scoped datastore state.

For public HDHomeRun output, an explicit JSON selector limits negotiation to JSON. A client that accepts only HTML receives 406. MIME matching accepts case variations. The HDHomeRun library passed 15 tests, and focused comparison with the retained Original negotiator matched all 85 cases.

Xtream live category filters now return an empty channel selection when a valid integer falls outside PostgreSQL’s signed 64-bit range. The panel still returns its full account and category document. The focused Rust test passed and 14 cases ran against pinned Django’s PostgreSQL query compiler without a database connection. The ARM64 fixture adds eight signed form and range checks.

## Guide Grid and operator compatibility

Future standard dummy Guide Grid windows keep the requested wall-clock offset for slot timestamps, programme IDs, and descriptions. The focused output test passed. The signed paired fixture adds ten requests per backend for future windows and checks unchanged fixture SQL.

The network-reset utility now rejects measured option-like tokens in the separated `--user` value position. The explicit `--user=--help` form still supplies the literal username. Five Rust operator tests passed, and the standalone parser comparison matched all twelve measured invocations of pinned Original. The final package fixture checks the actual CLI exit and datastore effects.

The Version200 scheduler owner evaluates four pinned Original outcomes in an independent ordinary Native clone in the same wave. It joins HTTP calls, broker task IDs, Redis results, saved counts, notification SQL and Channels events. The current focused suite passed 27 M6 tests and 34 coordinator tests offline. The final frozen run must supply the fresh worker and cleanup receipts.

## DVR callback and Category search

Saving DVR settings now uses pinned Original’s equality rules for offset values. An absent key and a null value compare equal, as do numeric-equivalent values. Equivalent saves avoid scheduling a rescan. Eighteen Core settings tests and the direct offset comparison tests passed. The paired fixture checks no-publication saves and a real change with its parent task, artwork tasks and future and shifted-past recording state.

Category searches accept the pinned Original single- and double-quoted search terms, including mixed words, and keep the existing term cap and bound SQL. The focused parser check passed. The existing browser cohort can verify the quoted queries under its signed movie-only and series-only roles.

## One frozen verification campaign

1. Integrate the reviewed product and workflow changes. Freeze a single Rust application Git revision and build its ordinary AMD64 and ARM64 packages. Record the committed input map, archive, actual manifests, image digests, React asset equality, licenses and attribution. Run `scripts/rust-milestone-batches/verify_release_packages.py` with the common AMD64 binding, independent ARM64 package audit, and both reviewed file digests. It checks that the two platform images use the same complete source map and rejects stale revisions.
2. Use the coordinator binding preflight for each phase. Each host uses one common candidate object for its local image. Owner contexts keep separate audit files, fixtures and output paths. Resolve unbound rows and replace partial probes with complete owners before launching the required phase.
3. Run the functional matrix on Beepboop and the assigned architecture and browser scopes on Lappy. The AMD64 coordinator keeps up to three ordinary fixtures in flight, protects shared reservations, and gives each heavy fixture the engine to itself. Lappy keeps its Compose projects and artifact custody independent. A test starts only once per sealed binding and phase.
4. Reconcile literal execution, teardown and evaluator receipts against the frozen revision. Required dependencies must be satisfied before later workflows. Run the two-hour soak and quiet equal-work benchmarks after their prerequisites. Compare CPU, working memory, useful throughput and first-byte latency under the recorded thresholds. Keep the previously deferred full-scan comparison separate.
5. Record the exact current result for every row and verify the same-application rollback and operator intake. Final deployment and default-switch approval are separate decisions after the release evidence is complete.

The coordinator’s multi-document binding check is an offline preparation step. Its result lists partial or missing inputs, while physical receipts establish release status. [Coordinator commands](../scripts/rust-milestone-batches/README.md)

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
