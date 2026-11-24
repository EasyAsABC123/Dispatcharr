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

### Focused checks while implementation moves

Keep a named selector and its actual selected-test count with each Rust
patch. Verify the compiler's path for the changed crate and its affected
workspace dependencies against the isolated checkout. A shared Cargo target
can retain metadata from another source snapshot. If a receipt shows that
mismatch, preserve it and refresh the affected sources in the isolated
checkout before building again. Keep unrelated owners' compiled output
available. Require the rebuilt test binary to list the exact test and record
its actual pass.

Use explicit Python test selectors for the public register as well. Its
entry module imports historical suites for the broad SDK run. The broad
offline run selected 384 tests, and 25 historical projection checks exposed
later evidence the reverse-projection helpers had yet to account for.
The repair regenerates the exact later attachments before removing them
and preserves the original historical hashes. All 25 failing selectors
passed in focused reruns. The whole SDK suite will run again with the
final candidate.

The executable register attaches scoped method receipts to Accounts
permissions, provider deletion preview, Core timezone choices and System
Events. Those attachments preserve the existing route IDs, dispositions
and packaged counts. The full pinned generator reproduced the updated
register byte-for-byte. [Executable contract register](rust-contract-register.json)

## Provider playback implementation

Grouped XC catch-up now derives the Original-compatible shared credential counter from the selected transformed login. Limited profiles use it alongside their profile counters during archive reservations, reopens and the read-only Redirect capacity check. Unlimited profiles bypass both counters. Grouped STD live playback accepts a sample with a plain query in the qualified format and preserves the same counter when the normal route selects FFmpeg or a configured input command. Ordinary grouped STD Redirect also carries this counter for both assigned stream selection and requests with a provider profile allowlist.

Grouped STD credential selection now checks typed live, movie, and series paths before stored account credentials, then checks short-form URLs. It decodes escaped short-form credentials once and preserves the raw typed-path spelling, matching Original’s two parsers. Ordinary media paths can use the stored login. A profile’s qualified literal or anchored rewrite applies to both the selected media URL and the pool sample. The final paired playback fixtures still need to verify live counters and release.

The catch-up and STD units passed, and the signed PostgreSQL resolver and real-Redis test targets compiled. The final paired workload will exercise shared capacity and release under playback, distinct profiles with the same login, an idle cross-worker reopen, and the direct and command-based STD formats. The existing preview, redirect, and transformed credential domain limits stay in the test plan.

## Channel Batch and VOD routes

Administrator Channel Batch now accepts ordered stream replacement and explicit clearing. Stream references are validated across the request before writing, surviving links retain their IDs, and the existing catch-up rollup updates the persisted channel. PATCH preserves the previously loaded catch-up values, which a fresh GET then reads from the updated channel.

Channel Batch also accepts raw channel numbers, TVG IDs, guide station IDs, and EPG assignments. It distinguishes explicit clears from omitted fields and preserves the existing compact-number behavior when a request also changes visibility. Raw EPG writes preserve Original's bulk-update behavior and skip guide publication.

For a top-level Channel Batch group edit, Rust now applies Original's
integer conversion before validating the group. This covers numeric strings,
Unicode decimal digits, and null and invalid inputs. Missing channels keep
their original error priority. Fourteen pinned conversion and DRF field
cases and the 24 focused Rust batch tests passed. The signed mixed-channel
replay still needs the final package.

Nested Channel Batch overrides support name, TVG ID, guide station ID,
channel number, and EPG assignments. An explicit null now clears a group
or radio override while an omitted field preserves its value. The existing
pruning step removes an override after its last value clears. Six pinned
Django component cases and the exact Rust update and prune SQL on private
PostgreSQL agreed on the retained rows. Guide changes invalidate the cache once and publish distinct programme jobs. For Schedules Direct, the plan skips guides with existing programmes, publishes individual jobs below three unfilled mappings per source, and publishes one mapped-guide job at the threshold.

The signed batch EPG assignment action uses that same Schedules Direct
dispatch rule. It checks publisher availability before changing assignments
that need new work. The existing populated mappings can still be assigned
without dispatch. Focused Rust tests and the exact query passed against
disposable PostgreSQL. The grouped Schedules Direct fixture also covers
nine retained cases, including the same-worker 90-second retry. The final
package must confirm the actual receipts and cleanup. The normal application binds the publisher to its registered Schedules Direct worker.

Numeric Movie, Series, Episode, Category, and Unified detail GETs and Movie, Series, Episode, Category, and Unified collection GETs accept Original's literal JSON suffix. M3U Accounts, EPG Sources, and Core Settings also accept that suffix on collection and numeric detail GETs. Numeric Notification detail GETs use the same per-user visibility rules and response serializer on the JSON suffix routes. Accounts Group detail and recurring DVR rule collection and detail GETs also accept the suffix through their existing access checks. The Connect integrations collection accepts the same suffix on its bounded administrator read. The aliases reuse existing authorization, SQL reads, negotiation, and credential filtering. Focused Store tests and the ordinary application build check passed. The final Compose fixtures still need to verify signed roles, persistence, and real guide job settlement.

The unsigned plugin repository owner can schedule the full 25-call comparison against the frozen candidate using the retained 24 Original observations and one retained 404. It independently checks application and fixture retirement, and 11 offline fixture controls passed. Full plugin lifecycle and performance retain their own release checks.

The signed plugin list negotiates JSON indentation and returns the measured
JSON 406 for unsupported media types. The current browser representation
continues through the same handler. The paired lifecycle owner will verify
the signed responses and their unchanged datastore and plugin state. The
list's OPTIONS display name uses Original's capitalization.

## HDHomeRun HTTP output profiles

HDHomeRun discovery, lineup, and status paths now accept percent-encoded decimal output-profile IDs. The router still requires ASCII digits after decoding and canonicalizes leading zeros, including on named ChannelProfile paths. Fifteen HDHomeRun library tests passed. The configured-client Compose fixture will check the ordinary signed package response and encoded aliases.

Registered public HDHomeRun data routes also accept the Original browser format on GET and HEAD, including named output profiles and the bare API aliases. Focused tests exercise content negotiation and preserve the existing authorization and device-data paths. The paired ARM64 owner will verify these requests on the final package.

Signed HDHomeRun device details also accept the numeric path spellings
that Django resolves, including percent encoding and Unicode decimals.
The datastore lookup uses the numeric key. Forty-eight pinned Django
component comparisons and sixteen focused Rust tests passed. Signed
packaged reads of the device aliases still need verification.

M3U, XMLTV, and HDHomeRun also share Origin URL formatting for trusted proxy headers. Empty optional forwarding headers fall back to the normal host or port. The existing per-request proxy trust check supplies that decision. Both output crates compiled and the shared formatter matched Original across 2,520 header combinations.

M3U, named XMLTV, and HDHomeRun now share the Original channel-number formatter. It preserves the fractional digits for binary64 ties in both displayed channel numbers and generated guide IDs. Focused Store, output, and all 15 HDHomeRun tests passed, and a separate comparison matched the pinned Python formatter across 49,990 finite inputs.

Xtream playlist and XMLTV requests with the single trailing slash now reach the same authenticated routes as `/get.php` and `/xmltv.php`. Both output crates compiled. The final signed fixture will compare the slash forms and their response bodies.

## First-admin setup on a clean install

The ordinary PostgreSQL service now emits the same ordered JSON setup documents as pinned Original. It shares the formatter with the copied SQLite runtime. The existing clean-install controller checks the exact status, media type and response bytes before the first administrator is created and after restart. It records a digest and length for each response. The focused PostgreSQL formatter, three copied SQLite setup tests and two controller checks passed.

The clean-install controller owns the browser from startup through
teardown. The Node launcher and its Chromium child share the fixture
process group. Seven isolated lifecycle controls verified exit, escalation,
profile cleanup and isolation from another process group. The final
immutable package still needs the real Chromium setup and restart run.

## SQLite backup creation and restore

The backup library now selects its snapshot from the admitted runtime datastore and writes both formats with the same bounded archive writer. PostgreSQL keeps the existing dump path. The optional SQLite path uses SQLite’s online backup API so the archive includes committed WAL data. Its version 2 archive contains `database.sqlite3` and `metadata.json`.

The optional restore path checks archive type and reads the staged SQLite catalogue before touching the target. With a quiesced datastore owner, it rejects open journal companions, retains the previous database as a rollback copy, and installs and verifies the recovered file. Linux passed 28 feature-enabled and 17 default backup tests.

The backup component uses the locked bundled SQLite library already used by the copied runtime's SQL driver. The copied runtime now answers a repeated setup POST when an administrator exists and exposes `/health/ready` with SQLite and Redis checks. On a migrated copy with no administrator, the same setup route can create the first account under the existing client IP policy. It uses the shared bootstrap parser and password worker, then writes an administrator row to the owned SQLite database. The copied-runtime Store tests passed, and the application and backup test targets linked with both optional features enabled.

The copied SQLite notification API now serves the unread count used by the React client. GET and HEAD share the bounded role, active-window, dismissal and developer-condition selection. Six SQLite notification tests passed, with one existing external-copy test waiting for its own fixture. The feature-enabled Main compiled.

The normal backup status route also returns Original’s HTTP 500 message for a decoded built-in `ValueError` with one plain string argument. Six Rust checks passed for the scoped type and byte limit. Actual task settlement, combined backup lifecycle and fresh packaged status replay still require their own receipts.

The ordinary package still needs SQLite startup, ownership wiring, migration compatibility, and image linkage. The SQLite component tests alone do not close M8 or M9.

## Recording and profile delivery owners

The one-off Beat row starts one signed recording fixture and verifies HTTP creation, the clocked SQL entry, one due-time broker publication, and its durable result. The scheduled 61-second recording row consumes that same owned fixture and evaluates media, recorder retirement, and cleanup independently. The coordinator admits both bindings before physical results exist and keeps their execution order. The known-default installation owner checks three ordered SQL receipts, reinstalls the removed status schedule with zero runs, and validates the other five rows and teardown against the retained Original roster. The Native profile-delivery owner reuses the two retained Original profile authorities, and the isolated controller validates the repository holding those receipts before admitting the binding. The coordinator and recording regressions passed offline, including recording admission before any physical receipt exists.

The hourly scheduler tick uses the same owned startup as the saved-default installer. It records both deliberate SQL due arms, ties them to two broker UUIDs and the intervening artwork child, and verifies the bounded publication time and terminal cleanup. The old Native journal remains useful for replay. Canonical credit requires the new due-arm receipts. The 21 M6 owner tests and 34 coordinator tests passed offline.

The ordinary scheduler normal-stop row reuses this fixture. Its evaluator requires the actual Main stop command, exit zero without OOM, unchanged run count, empty ready and reserved queues, and exact cleanup. The combined M6 owner suite passed 23 offline tests.

The EPG import route now attaches the declared POST and OPTIONS methods and the Accept variation to handled API responses. Front-door admission, over-limit, unsupported and uncaught server responses keep their own headers. A focused job-transport test passed, and the paired signed import owner still needs the actual worker, broker and cleanup receipts.

The first positive local XMLTV import now has its own evaluator using the existing paired two-import fixture. It checks the signed response, owned file, Original worker arguments, exact typed results, expiry and realtime updates. This uses the same paired package reservation as Guide and the second import. Thirty M6 and 34 coordinator tests passed offline.

## Provider deletion preview

The existing React provider deletion preview now has Source's
administrator JSON OPTIONS response for existing and missing accounts.
The focused run compared 34 signed and anonymous GET, HEAD and OPTIONS
calls with pinned Original renderer evidence. The nine fixture channel
rows stayed unchanged, and the private PostgreSQL instance retired.
Packaged ingress and HTML remain open.

## Guarded Core restore preflight

The ordinary Core scheduler now binds its read-only restore registry
check to the configured receiver and the same retained PostgreSQL writer.
A missing plugin bridge or registry releases the parked scheduler before
SQL reservation. With the required owners present, the preflight verifies
registry membership and closes the check before returning the existing
restore refusal.

Thirty-four focused Linux tests passed, including the exact missing-bridge
consumer. A private PostgreSQL run verified that a second scheduler cannot
take ownership until the actual elected session settles. The installed
plugin and builtin backup fixture is prepared for the signed package.
Restore apply and plugin-owned task execution remain open.

## Account permission catalogue

The administrator permission picker now receives Original's JSON OPTIONS
metadata. A HEAD request applies the account and role checks before giving
an administrator the 405 response. Pinned Django and DRF identified the
allowed methods and response bodies. Seventeen signed and anonymous calls
against a private PostgreSQL fixture verified the Rust route, including
unchanged permission rows. The packaged ingress and HTML cases remain open.

## Account API-key generation

The key and the returned user projection now come from the same
PostgreSQL transaction. The pinned Original component returned a
consistent saved response during a concurrent revoke. The Rust baseline
reproduced a mixed response when the revoke won the race. The patched
PostgreSQL component returned a consistent response, then allowed the
revoke to complete. Neither receipt retains generated key bytes. A
signed package and browser comparison remains open.

## Core timezone choices

The existing React timezone selector receives Original's JSON field order
and 433 timezone entries. The JSON OPTIONS route now returns Original's
metadata for authenticated users. Pinned Django, DRF and pytz checked
20 role and method cases. The Rust check compared the exact GET and
OPTIONS bodies and anonymous route responses. The signed package still
needs its grouped replay.

## System Events

The existing System Events API now accepts Python-style Unicode pagination
and exposes the administrator JSON OPTIONS response. HEAD and unsupported
methods authenticate before returning the method error. Handled pagination
failures keep the declared methods and Accept variation on their JSON
response. A pinned Original component checked 24 method, role and query
cases. Five focused Rust utility tests passed. The grouped signed Stats
workflow still needs its final package.

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

A definite broker refusal now runs the DVR offset rescheduler inline in ordinary Main, matching the Original fallback. The shared recording port serializes this branch with queued offset work before either reads the current settings. The inline run leaves parentless save-signal tasks. Ambiguous acknowledgements remain on the current error path, and the queued worker retains its parent. The outcome and lineage tests passed, and the combined app compiled. The final wave has separate live fixtures for an acknowledged refusal and for a lost reply after an actual enqueue.

Category searches accept the pinned Original single- and double-quoted search terms, including mixed words, and keep the existing term cap and bound SQL. The focused parser check passed. The existing browser cohort can verify the quoted queries under its signed movie-only and series-only roles.

## VOD artwork and Xtream information

Movie, Series, Episode, and VODLogo artwork now accept Original’s numeric URLs without a trailing slash. Twelve requests passed the focused router check. The provider-modal browser fixture adds paired signed SVG reads with both spellings and checks bytes, cache behavior, decoding and restoration of the owned provider state.

Signed Xtream Movie and Series information requests accept valid positive IDs written with leading zeros, an encoded plus sign, Unicode digits, or underscores. The focused gateway test and fourteen pinned Django query compiler controls passed. The paired fixture will check fresh provider lookups and byte-identical canonical warm responses.

For ordinary VOD refresh, a provider's JSON null list now reaches the
existing caught-error result path. The four Movie and Series actions each
report their Original error message. All four pinned Original component
cases and the matching local Rust HTTP cases passed. The final packaged
worker still needs its result, event and catalogue state checks.

Ordinary Xtream live import uses the existing empty-fetch behavior for
bounded provider responses that decode to a non-list. It keeps the
previous lineup and follows the same worker result path as an empty list.
Eight pinned Original and matching Rust loopback cases exercise these
responses. The final worker fixture must check the signed import result
and saved lineup in the same application context.

## Scheduler calendar and operator verbosity

The scheduler calendar owner now prepares a pinned Original fall-fold workflow. It records the first and repeated local 01:30, ties them to one publication and one durable SQL run, and checks the process and fixture retirement. The ordinary Native binder has checked Chicago and Newfoundland saved schedules offline. Fresh controlled daylight-saving transitions remain required to close the calendar row. The M6 owner suite passed all 34 offline tests.

The shipped `createsuperuser` parser also accepts attached short verbosity values from `-v0` through `-v3`. Three focused Rust parser tests passed and the standalone Original comparison matched all ten measured invocations. The signed operator fixture will check quiet output and durable admin creation.

The Original operator collector also accepts the seven pinned help-only
requests in the existing copied-database fixture. It records each command's
literal output, exit and process identity with bounded capture, alongside
the existing database, cache and Channels observations. Four tests passed,
including the pinned Django help path for the destructive command names.
Packaged Source capture and broader operator arguments remain scheduled
for release verification.

## Paired controls and worker coverage

The owner-local TS controls support switching to an assigned stream and
advancing through saved PostgreSQL channel assignments in order, including
wraparound. The switch keeps the existing viewer bodies through the
producer transition. Selecting the same direct stream again acknowledges
the request without redialing. Focused route, ticket, media-body and Main
checks passed. The paired fixture runs unlimited and one-slot phases in
the same application context per backend. It checks Redis ownership and
viewer continuity, then requires full retirement before changing capacity.
The local product also covers switching and Next between two eligible
unlimited direct profiles. It keeps the initial alias and releases the
previous reservation. When the immediate saved Next target has no active
default profile, the existing route reports the Original 404. The grouped
fixture checks owner, viewer and counter continuity through both positive
paths and the denial. The signed API can also request an assigned direct TS switch on another
eligible Rust worker. The producer checks the saved URL and its current
generation before switching. It confirms the new producer through the
existing media ticket and reports a receipt bound to the requesting worker.
A disposable Redis test checked generation fencing, pending cancellation,
receipt replay and cleanup. The final two-worker fixture still needs to
verify media continuity and retirement across both processes. It uses the same two viewer
responses and one upstream to exercise remote Next after the assigned
switch. Both workers recheck the immediate saved successor before the
producer opens it. A request-specific operation keeps older assigned-only
listeners from processing Next as an assigned switch. A signed remote
reselect can refresh the current stream when the stored URL still matches
the owned producer. The paired fixture checks that both viewers continue
and no new upstream connection opens. If the saved direct URL changes,
the owner verifies the open input before replacing it. The existing
producer and viewers keep their lease and stream ID. The final fixture
will require exactly one new upstream connection and decoded audio and
video after the edit. The same owner also exercises two direct saved
streams on one capacity-one provider profile. The switch retains the
current reservation, and the test checks that its count stays at one
through two viewers and returns to zero after both close. Next Stream now
uses that same held reservation for the immediate eligible saved
successor on another worker.

The ordinary Series DVR API can save a signed rule through the existing
DVR permission and settings writer. The worker evaluates a saved exact-title
rule, filters new-only episodes before deduplication, and can combine bounded
title and description predicates. The prepared comparison exercises disjoint
stations and overlapping rules on the same station. The first rule that
successfully books an episode keeps it. One paired owner keeps signed saves,
programme refreshes, booking history and artwork jobs together. Focused
Store and job tests passed, with signed worker and lifecycle receipts still
pending on the final package.

The ordinary recurring worker now settles supported one-date UTC rules
with zero bookings when the saved date and weekday fall outside the
active window. The focused checks cover past dates, the 14-day boundary,
weekday mismatches and the existing repeat behavior. The paired natural
tick still needs the real result and unchanged recording state.

The Redirect comparison reuses one project per backend for pooled framing,
one-slot fallback, saved three-stream ordering, and grouped STD previews.
It checks actual Redis reservations and origin requests. The DVR offset
comparison uses one signed settings update for three timestamp spellings
and their worker effects. Cold fixture controls passed. [Paired runner instructions](../scripts/rust-integration/recording-connect-cohort/README.md)

The mapped-file scanner now uses the pinned Original auto-import
defaults for absent and falsey settings. Eighteen Original decision
cases and six private PostgreSQL projection cases distinguished those
defaults from an explicit inactive choice and an oversized database
value. Two focused Rust tests passed.

The watcher now accepts lowercase suffix-only filenames and routes
`.m3u8` inputs to the existing plain playlist reader. Original admitted
nine measured filenames and rejected ten controls. Rust passed 36
scanner tests. The existing paired package fixture still needs the
watcher-to-worker handoff check.

An automatic Stream Settings update now maps a confirmed global rehash
broker rejection to the measured 500 response. A pinned Original
component confirmed that the setting remains saved and the owned locks
release after a definite rejection. The Rust transport controls keep a
lost or ambiguous reply distinct. The signed packaged PUT still needs verification.

The unchanged React manual rehash action now reaches the ordinary
Rust application. The administrator POST uses the saved global fields,
holds the same eligible account locks and returns Source's queued task
response or the manual conflict body. The pinned Original component
checked both responses. The focused Rust suite passed 21 tests. An
isolated signed replay then confirmed the manual route's exact JSON 400
for a real Redis permission refusal, with no queued task or result and
the request's locks released. The 409 retained the existing foreign
lock. The 200 queued one task with the returned UUID and the two
eligible account locks. Nine focused broker tests passed. The signed
packaged worker still needs verification.

The ordinary scanner skips Programme Index admission after its first
successful scan in the same process. Its grouped fixture runs the first
scan, a repeat while the Index table is locked, and a cold Main restart with
the same SQL and Redis. It expects two, zero, and two Index jobs and requires
a real database wait after restart. The five verifier controls passed offline.

The output verifier now checks the exact JSON bytes of four measured M3U
denials. The Connect log comparison checks the exact `Allow` header on
both JSON route spellings for signed role refusals and media negotiation.
Ordinary backup status returns Original’s observed HTTP 500 details for
successful task records containing a string, list or null result. Those
changes passed focused tests and still need the signed packaged comparison.

## Copied SQLite stream selections and saved schedules

Copied SQLite now accepts the existing React inline name PATCH within
the admitted manual one-stream channel scope. The write and returned
channel projection share one SQLite transaction. The tests checked the
signed denial paths, committed bytes after reopening the file, and
rollback after a projection failure. The 17 focused channel tests and
seven profile alias tests passed on both hosts. The Linux application
compiled with the copied runtime feature. Packaged execution and other
write domains remain open.

Copied SQLite accepts signed Channel Profile collection and detail JSON
aliases using the existing administrator and assigned-user scopes. The
suffix takes precedence over the format query. Twenty-five focused
tests passed, including the existing single-stream creation suite and
file-backed reads that preserved the database bytes. The signed
packaged route remains to be checked.

The copied SQLite backend now returns an ordered stream array for the signed
selection request that React makes when a selected row has moved off-page.
Six focused runtime tests passed. The existing projection keeps role-based URL
redaction and read budgets.

For one selected stream, the copied SQLite backend accepts the ordinary
signed administrator Channel creation request from React. It saves the
channel, assigned stream and supported profile memberships in one
transaction. The existing update bus publishes after commit. The focused
suite exercises omitted or null selection, one existing profile and an
explicit empty selection. Final packaged replay must verify the signed
route and real Redis and WebSocket delivery.

Ordinary Main now recognizes a saved, operator-named daily account-expiry
schedule under the existing interval, routing and argument guards. Twenty-six
focused scheduler tests passed. The final paired fixture needs to observe
the saved task name, publication, durable tick and notification effects.

Global Stream Settings, provider account validation, and the remote live
importer now trim the same field separators as the pinned Python parser.
Three Source controls and three focused Rust tests passed, with the
existing ordering, duplicates and fallback behavior exercised. The local
Standard worker now supports all five saved hash fields. Its admission
lookup and exact import projection produced the same pinned Python hash
for a composite input. The signed fixture adds a composite override to
its existing two-phase run. The local worker also uses the existing
ordered matcher for supported filter expressions. The first matching
name, URL or group rule decides eligibility before the hash lookup and
import. Pinned Source and isolated PostgreSQL checks agreed on twelve
assertions. The same signed fixture adds a case with one retained stream
and two exclusions. The shared matcher also accepts a leading Python
`(?i)` flag through the existing bounded case-insensitive set. Thirteen
pinned Original assertions and four focused Rust tests checked matching
and rule order. Other regex forms and the packaged comparison remain
open.

The local M3U worker honors a saved URL hash override and can use the
global setting for an absent, null or unrecognized override. Ordinary Beat
also admits the canonical saved URL override. For a refresh of a local
upload, explicitly disabling automatic live group enablement applies to new
relationships while preserving existing group toggles. The paired fixtures
will verify the saved configuration, result and persisted stream hashes.

The Programme Index writer settles the failed PostgreSQL transaction
and waits for its trailing reply before releasing the owned job. The final
fault fixture must show the job staying active while that reply is held.
Local XMLTV refreshes with a missing file or a readable nonempty truncated
file preserve guide data. After status and locks settle, the worker returns
the measured null result. Their paired API fixture shares the signed
publication, worker, Redis, SQL and event checks. The same owner can check
the initial forced local refresh when its saved TVG parse counter contains
plain malformed text. The worker records the observed error and returns
null after cleanup without acquiring the file lock or starting a child.
A local archive whose decoded XML is nonempty but has no closing guide
tag follows the same incomplete-file status path. Rust retains the last
validated cache and its pointer when this happens. Pinned Original
observations covered seven local archive and plain-file cases. Rust
passed 36 ingest-EPG tests, with one external-fixture check reserved.

Ordinary manual Channel creation accepts an explicit profile selection
with a supported guide when the request omits the stream list. Two focused
checks exercised the pinned Original serializer and signal order with and
without stream links. The paired owner includes the omitted-list request
with an existing dummy guide and verifies the saved membership, empty
stream list, cache generation and broker queue.

The Version worker decodes bounded stacked gzip and deflate in the
same order as Original. A paired component check used identical HTTP
response bytes. Rust cleared the obsolete notification and dismissal
rows and retained its Redis result through the held reply and shutdown.
The packaged worker still needs its signed comparison.

Recording recovery now treats falsey saved JSON properties as an empty
map for the active recording branch, matching Original. Eight Source
values and two focused Rust tests exercised the decision. A follow-up
matched the pinned Django metadata behavior for all ten stored shapes
in the snapshot, prime and finalization steps. A private PostgreSQL run
verified the distinct SQL NULL and JSON null paths and preserved a
concurrent STOP when the guarded write resumed. The signed packaged
worker still needs to recover a saved falsey row through restart.

An immediate XML TVG cancellation keeps its broker reservation until Redis
confirms the REVOKED result. The visibility sweep can recover the exact
request after an ambiguous reply. In a private Redis test, the result
committed while its reply was held. A fresh Rust Jobs connection later
recovered the same task without duplication and drained the reservation
after cancellation was reapplied. Whole-process recovery and the signed
operator path remain part of the frozen verification.

Closing worker intake now lets an already admitted XML TVG task make its
first retry publication. An old-versus-new test against private Redis
confirmed that the patched worker publishes one child with the original
lineage and records the parent as Deferred. The parent and its
retry now exchange atomically in Redis. A focused control confirmed that
a full queue retains the exact reserved parent and its visibility entry,
that a ready parent can be replaced without growing the queue, and that
an eligible exchange publishes exactly one child. In a separate private
Redis check the ready exchange committed and its reply was dropped. Rust
reported ambiguous publication without retrying. A fresh connection found
one linked child, the same sibling, and no invented terminal result.
The signed packaged worker still needs verification.

The Schedules Direct verifier now requires exactly one retry publication
for the deferred parent. It compares the recorded frame with the actual
reservation and checks the observed countdown against the 90-second ETA.
Three focused checks passed, including replay of the retained worker
receipt and rejection of duplicate or mismatched publications. Recovering
an already reserved retry after a cold worker restart remains open.

An interrupted plain HTTP M3U download with a bounded declared length
preserves both old cache files and publishes the same saved connection
error as Original. Focused loopback tests passed. The grouped remote-owner
fixture waits for observed download progress before closing the Origin
connection. It compares that progress with the worker's terminal result
and verifies the independently persisted account error and cache digests.
For an HTTP rejection before download progress, the Groups worker retains
its initial message and the account stores the provider's error separately.
The same paired owner includes a signed 403 case. Three loopback transfer
tests passed, and the pinned Original helper confirmed the outcome across
700 final HTTP error statuses in a private synthetic fixture. A complete
empty or HTML response follows the same separate account and Groups
reporting, with the prior raw and parsed caches preserved. The two
cases are part of the same signed fixture.

The ordinary remote Standard importer now groups 1,500 raw entries
before projection, matching the Original transaction boundary. The paired
fixture adds a 1,501-entry refresh with an owned write failure. It checks
rollback of the first batch, the last row's commit, retention of the
previous lineup, cache invalidation and worker settlement. It also checks
that the importer retries a failed batch once after all first-pass
transactions. A second signed refresh requires that retry to observe a
row from the successful sibling transaction.

## XMLTV source editor

The existing React edit can rename an active XMLTV source and change the
path of its URL on the same origin in one save. Rust updates both fields
in one transaction and retains the existing refresh schedule. The pinned
Original Django component kept the scheduled task and existing guide
index and queued no immediate refresh. A duplicate name returned 400.
Eleven focused Rust tests passed. Six signed calls against private
PostgreSQL confirmed the successful edit, role denial, unchanged index
and duplicate-name rollback. The packaged ingress and React replay
still need verification.

## Programme search

The ordinary Rust programme search now returns role-scoped pages,
including channels linked through a guide override. It applies the stored
date and guide filters and uses the existing title and description
parser for quoted phrases, AND and OR, grouping, whole words and regex.
Eleven pinned Original view cases and twelve parser cases ran against
owned fixtures. The focused Rust suite and a real private PostgreSQL
test checked projection, paging, role visibility and text matching.
Channel name, channel ID, stream name and group filters each select
effective guides independently. The resulting set is intersected with
the guides visible to the requesting role. The response includes every
visible channel and stream
linked to a matching guide. The pinned Original passed 28 relation
requests. A private PostgreSQL run checked the new Rust filters across
both roles, including override precedence and intersections across
visible channels.

The ordinary and literal JSON search routes give administrator users
the pinned Original's OPTIONS metadata. The same role check applies
before the method error for an unsupported request. Standard users
can use canonical GET and HEAD. With the literal JSON suffix, a read
that passes authentication reproduces Original's HTML 500. The
Django and DRF component confirmed 48 method, spelling and role
combinations. The Rust checks covered routing, unauthenticated
negotiation, HEAD body handling and exact metadata. The current signed
Guide package owner is ready for grouped verification.

## Channel logo pagination and Xtream information

The ordinary PostgreSQL Logo API uses the same Python-style integer parsing
for pagination as the copied SQLite API. Supplied forms include Unicode
decimal digits, underscores and explicit plus signs. The existing paired
Logo fixture adds six signed cases and checks exact JSON bytes alongside its
existing SQL, file and Origin checks. Copied SQLite also supports repeated
logo IDs and name filters, including literal SQL wildcard characters.
Eight pinned Django SQLite checks and ten focused Rust tests passed.

Ordinary Xtream Movie and Series information GETs with an omitted ID now
pass through the existing authenticated lookup to the measured HTTP 404.
The paired Xtream fixture reuses its signed catalogue and metadata owner.
It checks the two missing-ID cases for exact response bytes and no further
provider or datastore effects. The same fixture adds signed zero, negative
and malformed identifiers. Pinned Original view and database components
established the expected responses. The final packaged run will verify
their wire behavior on the common revision. For valid integers outside
PostgreSQL bigint, Rust returns the same 404 before the relation lookup.
Twenty-four pinned Original view calls used an isolated PostgreSQL server
and produced the same response. The Rust library passed 67 focused
tests, with 25 external-fixture tests reserved for owned runtime checks.
The existing paired owner adds eight signed overflow cases.

The Xtream panel sends Original's HTML 500 page for a malformed category
integer. Twelve pinned Original panel calls checked malformed and valid
requests on both route spellings. The Rust library passed 67 tests, and
the same ARM64 output owner adds six signed malformed-category checks.

## Named M3U and XMLTV profile URLs

The output gateways accept one percent-encoded trailing slash as the
optional delimiter for a named profile. Pinned nginx and Django checks
confirmed the same profile selection and six M3U query results. The
two Rust libraries passed 35 focused tests. Twenty-four external
fixture tests were reserved. The combined ARM64 owner adds two paired
wire comparisons to its existing output and HDHomeRun checks.

An M3U POST that passes the ordinary request checks now matches
Original's response when its body cannot be decoded as UTF-8. The
pinned Django component checked ten cases across the named playlist
and Xtream export. It returned HTML 500 for the decoding failure
and 403 for valid nonempty UTF-8 bodies other than the accepted empty
object. A focused Rust test matched the exact failure bytes. The
ARM64 paired package still needs its signed replay.

## HDHomeRun metadata

Public HDHomeRun OPTIONS metadata uses the selected JSON indentation
with the eight-space cap and the measured device XML name. Forty-eight
pinned Original view calls checked the renderer across four public views.
Seventeen Rust library tests passed and three external-fixture cases
remain reserved. The existing ARM64 paired owner adds twelve signed
or public wire checks and compares exact status, headers and body.

## Retained Original TVG and Version authority

The coordinator accepts the exact retained Original dependency for six
TVG and four Version200 cases. It verifies all 221 admitted source evidence
files, reruns the two scoped reducers and checks the recorded cleanup.
The first epoch retains its exit 1 after the reviewed TVG prefix. The
Version epoch retains its exit 0. Admission forbids replay and requires
separate Native results for each dependent workflow. All 34 coordinator
controls passed on the combined scripts.

## Scheduled Version check with an upstream 503

The coordinator can dispatch the ordinary Native Version503 case after
the account warning and expiry check. The retained Original evidence links
one scheduled task to the upstream HTTP 503, a successful null result,
and exact cleanup. The Native fixture reuses the existing disconnected
Version owner and verifies its actual outcome and cleanup. Seventeen
offline controls and the 34 coordinator tests passed. The final Native
package run remains open.

## Recording with a held PostgreSQL ReadyForQuery

The M6 coordinator can dispatch the recording check that holds
ReadyForQuery on the same PostgreSQL connection after a real error.
It is bound to the reviewed null-safe recording update. The later
physical run must prove the error and both held samples, exact
connection settlement, the matching task result, decoded audio and
video, and both project teardowns. Original runs the same ordinary
recording workload. The fault is applied in Native. Ten offline
controls and the 34 coordinator tests passed. The final package
and physical execution remain pending.

## Saved Stats and ghost cleanup

The M6 coordinator can dispatch the ordinary saved Stats viewer and ghost
cleanup check after its backend-cleanup prerequisite. It uses the existing
private lease and one Native project, including a same-data restart. The
pinned Original ran both saved schedules, kept signed media playing during
ghost removal, and delivered the Stats WebSocket frames. The fresh Native
check requires typed before-and-after snapshots, real media and socket
work, and exact termination and teardown. An older Native receipt lacked
the required snapshots and was rejected. All 34 coordinator controls passed.
Packaged execution and the rest of the M6 job graph remain open.

## Historical measured performance

The earlier quiet comparisons use pinned Original `1241b3f7` and the
specific Rust revisions named in their receipts. The latest migration
branch needs its own frozen measurements.

- At `2d3ffe53`, the AMD64 live MPEG-TS tests covered 1, 10 and 100 signed
  viewers with at most one simultaneous origin connection. Median paired
  CPU fell 38.5%, 57.7% and 87.4%. Peak working memory fell 49.9%, 44.6%
  and 47.6%. [M1 live comparison](rust-m1-2d3ffe53-amd64-signed-live-performance.md)
- At `2d3ffe53`, the two-viewer ARM64 HLS comparison recorded 23.2% lower
  CPU and 75.0% lower peak working memory. Both arms decoded identical
  media and shared one upstream. [M3 HLS comparison](rust-m3-2d3ffe53-arm64-signed-hls-performance.md)
- At `2d3ffe53`, the AMD64 VOD prefix comparison recorded 5.91% lower
  paired CPU and 34.29% lower peak working memory. First-byte p95 moved
  from 266.46 ms to 258.62 ms. [M4 VOD comparison](rust-m4-2d3ffe53-amd64-vod-performance.md)
- The earlier quiet Auto Sync full-scan repeat saved 24.68% paired CPU
  and 54.95% peak working memory. One pair missed its latency gate.
  Rust p95 was 65.27 ms versus 56.46 ms for Original, a 15.6% increase.
  The first-byte comparison also missed its gate in that pair. The cause
  remains unconfirmed. The full-scan rerun remains a separate final gate.
  [Full-scan repeat receipt](../scripts/rust-perf/candidate102-regex-preview/README.md)

## One frozen verification campaign

1. Integrate the reviewed product and workflow changes. Freeze a single Rust application Git revision and build its ordinary AMD64 and ARM64 packages. Record the committed input map, archive, actual manifests, image digests, React asset equality, licenses and attribution. Run `scripts/rust-milestone-batches/verify_release_packages.py` with the common AMD64 binding, independent ARM64 package audit, and both reviewed file digests. It checks that the two platform images use the same complete source map and rejects stale revisions.
2. Use the coordinator binding preflight for each phase. Each host uses one common candidate object for its local image. Owner contexts keep separate audit files, fixtures and output paths. Resolve unbound rows and replace partial probes with complete owners before launching the required phase.
3. Run the functional matrix on Beepboop and the assigned architecture and browser scopes on Lappy. The AMD64 coordinator keeps up to three ordinary fixtures in flight, protects shared reservations, and gives each heavy fixture the engine to itself. Lappy keeps its Compose projects and artifact custody independent. A test starts only once per sealed binding and phase.
4. Reconcile literal execution, teardown and evaluator receipts against the frozen revision. Required dependencies must be satisfied before later workflows. Run the two-hour soak and quiet equal-work benchmarks after their prerequisites. Compare CPU, working memory, useful throughput and first-byte latency under the recorded thresholds. Keep the previously deferred full-scan comparison separate.
5. Record the exact current result for every row and verify the same-application rollback and operator intake. Final deployment and default-switch approval are separate decisions after the release evidence is complete.

The M6 playback-during-refresh probe can now bind a reviewed read-only
fixture through the coordinator. The planned paired phase uses one
ordinary application context per backend for two signed viewers,
a held and released XMLTV refresh, a second guide import, and the
strict timed recording. Its offline admission checks preserve the same
127 matrix rows and the durable retry, cancellation and restart
prerequisite. A successful run supplies scoped evidence for this
workflow while the release row remains open.

The coordinator’s multi-document binding check is an offline preparation step. Its result lists partial or missing inputs, while physical receipts establish release status. [Coordinator commands](../scripts/rust-milestone-batches/README.md)

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)

### Channel Group action metadata and copied SQLite channel name replacement

The canonical Channel Group cleanup and unclaim actions now expose their JSON
`OPTIONS` metadata. Pinned Original Dispatcharr with Django 6.0.9 and DRF 3.18.3
served 16 isolated requests across collection, claim, unclaim and cleanup. Each
new action returned 200 for administrators, 403 for Standard and Streamer, and
401 for anonymous. Both preserve `POST, OPTIONS` and `Vary: Accept`. The Rust
tests compare the two exact administrator JSON bodies and exercise anonymous
router requests. The canonical source route IDs are
`route-73e3a2ec965d642de991` and `route-43655576925d1457c856`.

The copied SQLite companion accepts a canonical name-only JSON `PUT` for the
same bounded manual, single-stream channel shape as the existing name edit.
Missing name returns the exact required-field response without writing. A
successful update changes name and its modification timestamp in one transaction.
The disposable-file tests survey stored rows and SQLite types across all 63
captured tables and verify the updated name after close and reopen. Retained
Original evidence covers required-name refusal and a broader scalar replacement.
The copied SQLite name-only implementation is supported by the pinned Original
serializer's treatment of omitted optional fields and stream links.

Pinned Linux Rust checks cover the copied-data Main build, both Channel Group
metadata tests, 19 copied SQLite create and name-edit tests, and seven profile
alias reads. Signed packaged requests, browser and WebSocket coverage, and
same-package restart remain in the final acceptance fixtures.

The original Dispatcharr authors, Django, Django REST framework, SQLite and
dependency contributors retain full credit and their existing licenses.

### Bulk stream lookup

The preserved React API helper can send more than 50 stream IDs to the bulk
`POST` action. Rust exposes the canonical route and both JSON suffix
spellings. The shared direct selector retains model update order and includes
inactive-provider rows, with source URLs visible only to administrators.
Bulk queries with no matches return an empty array, and the small-list GET
retains its 404 behavior.

Pinned Original Django and Django REST framework covered 13 actual POST
requests and 17 OPTIONS requests. Administrator OPTIONS includes metadata for
23 fields, with the same compact JSON bytes across all three paths, plus
tested indented output. Standard and Streamer receive 403 for OPTIONS, and
anonymous receives 401. The isolated Unix-only PostgreSQL fixture matched
both full 51-row administrator and Standard POST response fingerprints.
Signed requests on the final package, browser execution and non-JSON
representations remain pending.

The original Dispatcharr authors, Django, Django REST framework and
dependency contributors retain full credit and their existing licenses.
