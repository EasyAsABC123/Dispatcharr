# Rust migration milestones and acceptance gates

The target is the complete backend behavior at Python revision
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`, with the existing React frontend.
[Architecture and compatibility inventory](rust-migration.md) explains the scope.
A milestone passes only when its implementation, contract tests, container run,
and performance evidence all pass. A scaffold, unit test, or fast synthetic buffer
alone does not establish application parity or an application performance gain.

## Current integrated work

The [9cf6407 HTTP attachment packets](../scripts/rust-contracts/observed-http-9cf6407/README.md)
record 16 Channels and M3U directory requests and 16 Programme Guide requests. Portable
offline verification covers the tested route identities, response projections and physical cleanup. The
[executable contract register](rust-contract-register.md) now binds the 16 Programme
Guide cases to the two declared list and detail GET rows as partial evidence.

The [9cf6407 profiled HLS pair](rust-m3-9cf6407-profiled-hls-performance.md) matched
30 complete segments with 32.4% lower CPU and 47.4% lower peak working memory.

The [9cf64 completed-DVR comparison](rust-m4-9cf64-completed-dvr-performance.md)
passed all six pairs with identical media and equal goodput. At ten viewers Rust
used 64.2% less CPU and 58.7% less peak working memory across the fixture.

The [5d9568f explicit worker rehearsal](rust-m6-5d9568f-explicit-role-compose.md)
verified startup, HTTP XMLTV import and descendants, natural scan and cleanup,
durability after retirement, and a ready peer with exact teardown.

The [f960875 short soak](rust-m9-f960875-sqlx-idle-correction.md) passed the original
120-second workload and post-playback resource budgets after preserving SQLx idle age.
Its [Native AIO profile](rust-m6-f960875-native-aio-profile.md) independently verified
worker startup, the import and scheduler tasks, durable ownership release, and a
ready modular peer. The 360-second prefix and two-hour soak have separate gates.

The [08d3b455 media wave](rust-m3-m4-08d3-current-media.md) matched the active
catch-up position and decoded range across both backends and verified a fresh
fMP4 request on the ready Rust peer after planned lead retirement. The
[stock Original AIO witness](rust-m6-original-stock-aio-job-graph.md) verified
both worker queues, startup jobs and one natural Beat cleanup.

The [4d3cfbb EPG library pair](rust-m0-4d3cfbb-epg-library-get.md) matched eight
successful JSON GETs per backend across four operations, including the Original
headers and body digests, with unchanged guide SQL and exact cleanup. The route
register now records 17 partial operation bindings and 427 observations. The
four added operations carry partial coverage for the eight measured cases.

The [Connect formatted log-list GET](rust-m0-e7dda7f-connect-log-json.md) now has ten scoped attachments at resolver ordinal 347. The register records 18 partially bound operations and 437 mapped packaged observations. All 322 unresolved declaration work items and all whole-operation verification flags retain their previous states. Browsable HTML and API-key cases remain open for this GET.

The [scoped Compose wave](rust-m5-m7-m8-scoped-compose.md) verified the manual
channel edit on `5d9568f`, and realtime and ordinary plugin settings on `1124391`.
Each workflow passed its owned cleanup checks.

The [08d3b455 grouped STD pool pair](rust-m2-08d3b455-std-sample-pool.md)
matched four Original and Rust calls, including shared-capacity refusal, a distinct
pool and shared admission after release. Both arms verified media and teardown.

The [fixed-path BuildKit experiment](rust-m10-fixed-path-build-comparison.md)
measured warm full-image build time on the same source and checked identical cold
and warm executable bytes. The [runtime smoke](rust-m10-fixed-path-runtime-smoke.md)
matched 40,209 selected paths across the ordinary and both pilot images and passed clean
install and shared playback. The [Original STD pool reference](rust-m2-original-std-sample-pool.md)
verified actual shared-capacity refusal and admission after release.

The [5d9568f explicit scheduler restart](rust-m6-5d9568f-retained-producer.md)
preserved one Native-published pending task across producer replacement and
verified completion by a fresh receiver, with exact cleanup.

The [1124391 export comparisons](rust-m7-1124391-ordinary-exports.md) passed 19 M3U
and 13 XMLTV cases per arm with independent cleanup. The
[65dbbbd short-soak repeat](rust-m9-65dbbbd-short-soak-resource-stop.md) observed
seven persistent PostgreSQL processes whose sockets matched the same Native Main. A later
pool-state diagnostic linked all seven to SQLx maintenance resetting their idle
timestamps. The original short resource gate remains open.

The dedicated [f960875 Movie and Series provider-modal replay](rust-m9-f960875-provider-modal-browser.md)
passed both real Chromium arms with unchanged React and exact cleanup. The
[local copied PostgreSQL rollback](rust-m10-m11-f960875-local-rollback.md)
reconciled one accepted write through the warm Original endpoint in 7.929 s.

Fresh native ARM64 projects matched the [ordinary account API](rust-m5-f960875-arm64-ordinary-account-pair.md)
and verified [both owned AIO startup task terminals](rust-m6-f960875-arm64-aio-downstream-startup.md).

The exact ordinary AMD64 [e68fb95 successor](rust-m0-m6-e68fb95-appdata-and-named-schedules.md)
matches the measured numeric AppData Allow/Vary fields and runs both separately
named Version rows under ordinary AIO ownership. The two empty DELETE 204
responses retain their original framing difference. The named cleanup shape
passed three [e68fb95 checkpoints](rust-m6-e68fb95-named-cleanup-checkpoints.md) under
ordinary AIO, covering current and overdue at boot and overdue after readiness.
The [native ARM64 e68fb95 package](rust-m10-e68fb95-native-arm64-package.md)
passed exact binary, default-UID, startup, React HTML, and scoped authentication checks.

Its [ordinary ARM64 AIO replays](rust-m6-e68fb95-arm64-aio-startup.md) verified
seven saved schedule contracts, a natural scan tick and both owned startup
task results. The [e68fb95 ARM64 startup diagnostic](rust-m8-e68fb95-arm64-startup-role-delta.md) retains observed Beat table inserts and the different qualified roles. On f960875, the [Version consumer fixture](rust-m6-f960875-ordinary-version-consumers.md)
matched six retained Original cases and verified exact cleanup. The [copied
Cleanup checkpoint evidence](rust-m6-f960875-preboot-cleanup-recovered.md)
preserves the overdue controller's exit 1 and the successful independent
reduction of its observed SQL, broker and cleanup effects.

The fresh [e68fb95 ten-viewer XMLTV pair](rust-m2-e68fb95-live-xmltv-performance.md)
measured 55.3% lower backend and infrastructure CPU and 78.3% lower peak working
set on AMD64. Both arms served ten progressing viewers through one upstream
connection and completed the same XMLTV job and media decode.
The [f960875 paired direct M3U replay](rust-m7-f960875-direct-provider-m3u.md)
matched four owned provider selection phases. Lappy concurrently ran three
[isolated ARM64 fixtures](rust-m9-f960875-arm64-parallel-compose.md), with five
observed three-project samples, one generated-output pass, two failed fixtures
and verified teardown in all three.

Lappy's [fresh concurrent repeat](rust-m9-f960875-arm64-three-fixtures-pass.md)
passed all three isolated lanes with five overlapping samples and exact teardown.
The [paired real React browser proof](rust-m5-f960875-arm64-account-browser-pair.md)
covers initial account creation, sign-in, authenticated rendering, profile
read and sign-out on both backends.

The [e68fb95 streaming core and runtime](rust-m1-e68fb95-bounded-stream-core.md)
passed 33 deterministic host tests. The [f960875 growing DVR replay](rust-m4-f960875-growing-dvr-playback.md)
passed on both images with two playlist generations and matching segment bytes.

The [88b9f45 real clocked DVR pair](rust-m4-88b9f45-clocked-producer.md)
matched all 2,862 encoded packets in its completed recordings, including
payload and timing. The [f960875 active OutputProfile probes](rust-m9-f960875-active-profile-status.md)
matched the initial TS 200 and HLS 302 responses and verified exact cleanup.
Original and Native also passed the [ARM64 same-schema restore comparison](rust-m8-f960875-arm64-same-schema-restore-pair.md)
in separately owned PostgreSQL environments.

The [method declaration map](../scripts/rust-contracts/method-declarations/README.md)
accounts for all 322 unresolved-route work IDs and identifies 98 explicit Original
method declarations. An exact [e68fb95 backup GET replay](rust-m0-e68fb95-backup-readonly.md)
passed inventory, direct owned archive download, repeated readback and cleanup
on both backends.

The [direct ModelAdmin diagnostic](rust-m0-e68fb95-modeladmin-readonly-diagnostic.md)
confirmed Original administrator HTML 200 for two stock list and history
registrations. The same Native paths returned its 404/301 ingress outcomes.
Dedicated admin parity remains open.

The [stock listener comparison](rust-m0-stock-aio-modeladmin-boundary.md)
confirmed that both anonymous list paths reach Django through nginx, while
public login and history use the nginx redirect. The [owned trusted plugin
actions](rust-m8-f960875-aio-trusted-plugin-actions.md) passed on ordinary
f960875 Native AIO against retained Original responses.

The [f960875 abrupt Native fMP4 observation](rust-m3-f960875-native-abrupt-fmp4.md)
decoded both retained viewer prefixes and recorded the attached follower's EOF.
Both backends passed the [six-default cold and warm scheduler rehearsal](rust-m6-f960875-cold-warm-scheduler-install.md)
and the [assigned-number M3U, HDHomeRun and XMLTV comparison](rust-m7-f960875-assigned-numbers-guide.md).

The [f960875 ordinary React API read wave](rust-m0-f960875-react-read-wave.md)
matched eight EPG library reads and four Channel Profile reads per backend
with preserved scoped SQL and exact cleanup.

The [f960875 planned fMP4 replay](rust-m3-f960875-planned-fmp4-handoff.md) passed
on Original and Native after the lead viewer disconnected and both media leases
drained. Each backend decoded both clips and admitted a fresh viewer on the
peer. The [ARM64 standalone restore replay](rust-m8-f960875-arm64-standalone-restore.md)
passed with a synthetic current-schema PostgreSQL archive, restored account
and SQL readback, an unchanged independent control database, and exact cleanup.
The AMD64 [AppData comparisons](rust-m0-m5-f960875-numeric-app-data.md)
matched scoped SQL, roles, statuses, and bodies while retaining specific wire-header differences.

The [f960875 two-hour soak](rust-m9-f960875-two-hour-soak.md) passed with ten viewers,
12 scheduled XMLTV imports, 140 decoded clips, and exact cleanup. The release
workflow-parity prerequisite and whole-service comparison remain open.

The [f960875 workflow wave and soak prefix](rust-m9-f960875-functional-wave-and-soak-prefix.md)
verified 16 fresh workflows and a separate 360-second soak on one exact image.
The two-hour soak and full 127-row release suite remain open.

The [1124391 grouped Compose run](rust-m9-1124391-sixteen-compose-workflows.md)
passed 16 selected workflows, with three retained Original authorities, 108 blocked
rows and exact cleanup. The full 127-row gate remains open.

The [1124391 Guide and mixed Beat scope](rust-m0-m6-1124391-compose.md) passed
ten Programme Guide cases per backend and verified one publisher per scheduled
occurrence across Selected/Core retirement and Legacy acquisition.

The [65dbbbd fMP4 pair](rust-m3-65dbbbd-profiled-fmp4.md) confirmed two overlapping
readers on one upstream, one transform, and one remux. The remaining reader continued
after the lead reader detached.

The [65dbbbd standalone restore](rust-m8-65dbbbd-standalone-restore.md) used a
genuine Original archive and verified copied database rows, signed task completion,
same-process reads and teardown.

The [65dbbbd short soak](rust-m9-65dbbbd-short-soak-resource-stop.md) completed both guide
refreshes and stopped on post-playback process and descriptor limits. A separate
[Core shutdown SQL hold](rust-m6-9cf6407-sql-release.md) passed timely release and handoff.

The [9cf6407 shared-viewing XMLTV benchmark](rust-m2-9cf6407-live-xmltv-performance.md)
kept ten viewers progressing during one real import. One measured pair recorded 52.9%
less backend and infrastructure CPU and 79.7% less peak working memory on Rust.

The [9cf6407 scheduled Version and recurring recording pairs](rust-m6-9cf6407-version-recurring-compose.md)
verified four Version branches and a recurring recording run and repeat. The same image
passed the scoped [React provider modal pair](rust-provider-modal-browser-9cf64.md).

The [65dbbbd HDHomeRun successor](rust-m0-65dbbbd-browser-options.md) matches four
ordinary and forced browser OPTIONS responses on the fixed status route across both
network policies. The evidence includes the exact ordinary package and owned cleanup.

The [ordinary M7/M8 Compose comparisons](rust-m7-m8-9cf6407-ordinary-compose.md)
matched 19 M3U and 13 XMLTV cases per backend, plus real PostgreSQL backup inventory
and exact download output on the same `9cf6407` image.

The [checkout-only Clean Install replay](rust-m9-local-clean-install/README.md) rebuilds
the exact `9cf6407` application inputs, runs the tracked React bootstrap and restart
workflow, and checks the local image and exact Compose cleanup.

The [focused M5 and M6 Compose pair](rust-m5-m6-9cf6407-focused-compose.md) verified the
Channel Groups lifecycle across serving restarts and the naturally due stock scanner
with its genuine M3U group child. Both comparisons passed exact cleanup.

The [current 9cf6407 workflow evidence](rust-m9-9cf6407-seventeen-workflows.md) links
17 distinct fresh passes across three scoped Compose waves. The latest checks cover Guide
Grid, shared streaming including 100-viewer retention, and two scoped operator suites.

The [combined 9cf6407 workflow evidence](rust-m9-9cf6407-eleven-workflows.md) links
eleven distinct fresh passes across two scoped Compose waves, including clean install,
React/Stats, profiles, and VOD/timeshift. The formal 127-row release gate remains open.

Four [required M9 Compose rows](rust-m9-9cf6407-four-required-compose.md) passed fresh on
9cf6407, including Complete Refresh with its immediately preceding Original reference. The
same image passed a [local warm rollback](rust-m10-m11-9cf6407-local-rollback.md) with one
acknowledged Native write reconciled on copied Original data. The full release gate remains open.

The [9cf6407 ordinary checkpoint](rust-m0-m6-9cf6407-compose.md) verified the two fixed
HDHomeRun browser OPTIONS cases and an opt-in Selected/Core warm handoff. The same peer
took over the scheduler and ran the second naturally due cleanup after the leader retired.

The [ad6babd ordinary checkpoint](rust-m0-m6-ad6babd-compose.md) verified
saved XC live refresh and automatic VOD ingestion, a linked UTC XMLTV cron,
six deterministic HDHomeRun HEAD and JSON OPTIONS cases, and the post-commit
branch of manual channel creation with a real XMLTV guide. Its scheduler
checks used explicit Selected/Core ownership.

The [ad6babd guide-create timing replay](rust-m0-m6-ad6babd-compose.md#guide-channel-creation)
matched the Original in both measured transaction orders. The pre-commit
programme job kept existing programme rows and invalidated the cache once.
The post-commit job replaced programme rows and invalidated it twice.

The [ad6babd shared-viewing comparison](rust-m2-ad6babd-viewing100.md)
measured three fresh 100-viewer pairs with one upstream connection and
one provider slot per backend. The six phases passed useful audio and video,
resource and throughput budgets, and exact cleanup. Active jobs and
release-wide performance retain their own gates.

The [a99e1772 ordinary checkpoint](rust-m0-m9-a99e1772-compose.md)
matched all 224 reviewed requests to the fixed HDHomeRun lineup status GET.
The [standalone verifier](../scripts/rust-contracts/observed-hdhr-get-a99e/README.md)
rechecks 208 raw wire pairs and 16 browser responses against their two
Original HTML witnesses. The same image passed a saved local M3U interval
and a copied clocked DVR booking through explicit Selected/Core, with
playable audio and video and exact teardown.

The [a99e1772 required Compose wave](rust-m9-a99e1772-required-seven.md)
passed seven fresh workflows. Two retained Original dependencies supported
Native profile delivery, and 118 rows were blocked in this scoped wave.
Release-wide parity, performance, soak, and cutover remain open.

The [guide workflows and HDHomeRun observations](rust-m0-m7-a99e-observations.md)
record the fresh 17-route metadata pair and Original evidence for linked
XMLTV cron, channel creation with a real guide, and fixed HEAD/OPTIONS.

The [949c864f runtime checkpoint](rust-m0-m6-949c864f-runtime-checkpoint.md)
verified inherited Redis with one scheduler owner and a web peer, recurring
recording repeat, account expiration, Newfoundland cleanup, profile restart,
and ordinary backup inventory and direct download.

The [949c864f ordinary checkpoint](rust-m0-m6-949c864f-stats-and-hdhr.md)
matched two receiver-backed saved Stats schedules and eight browsable
HDHomeRun authentication cases with exact retirement. Its predecessor
also matched a Newfoundland cleanup occurrence in explicit Selected/Core mode.

The [e0016d42 HDHomeRun browser authentication comparison](rust-m0-e0016d42-hdhr-browser-auth.md)
matched eight 401 statuses and bearer challenges. Original used its
browsable HTML response and Rust used JSON. Both scopes retired cleanly.

The [e0016d42 ordinary and copied-data checkpoint](rust-m6-m8-e0016d42-scheduler-copied-preview.md)
matched saved schedule identities, Scanner and Recurring work, four Version
outcomes, and daily expiration. The copied-feature image also served two
real Standard-user viewers, including unchanged React Preview, through
one upstream with decoded media and exact retirement.

The [c1b52dc8 formal Compose wave](rust-m9-c1b52dc8-profile-browser-wave.md)
passed clean install, live playback with Stats in unchanged React, and
Native profile delivery. It verified 16 fresh A/V observations and exact
retirement. Two retained Original dependencies supported the profile check.

The [c1b52dc8 Selected/Core scheduler](rust-m6-c1b52dc8-selected-core-known-defaults.md)
verified one assigned background owner, a ready web peer, real startup
work and scheduled cleanup. The paired defaults fixture passed Scanner and
Recurring Maintenance. Its restart observer found six Rust identity
sequence increments compared with zero on Original.

The [66b02c4a grouped Native media run](rust-m3-66b02c4a-native-profile-delivery-cohort.md)
passed HLS, fMP4, equivalent and distinct profiles, fallback, and fresh
FFmpeg child recovery, then exercised VLC in the same Compose fixture.
Sixteen A/V observations and exact teardown passed. Formal coordinator
qualification remains open.

The [66b02c4a Native FFmpeg recovery](rust-m3-66b02c4a-profile-recovery.md)
verified fresh profile playback after owned child loss. The unprofiled
viewer continued on one upstream. Exact child and Compose cleanup passed.

The [66b02c4a checkpoint](rust-m0-m6-66b02c4a-xmltv-hdhr-checkpoint.md)
matched two linked saved XMLTV sources and their four causal children.
The same ordinary image also matched both anonymous HDHomeRun status GET
wire responses with exact cleanup.

The [Original media and profile reference](rust-m9-original-media-profile-workflows.md)
verified 16 A/V observations, natural source switch, FFmpeg/VLC profiles,
and recovery on a fresh request. It supplies the retained Original media
dependency for the coordinator-owned Native profile delivery row.

The [66b02c4a grouped Core preview](rust-m6-66b02c4a-core-cleanup-version-preview.md)
matched saved Version HTTP 503, scheduled backend cleanup, and normal
stop in one Compose fixture per backend. The same image qualified both
scopes through the explicit Core preview.

The [7f6b7016 scheduled-backup pilot](rust-m6-m8-7f6b7016-selected-core-beat.md)
matched the ordinary saved UTC backup workflow using one Selected worker
and scoped Core Beat, with exact cleanup. The explicit mode remains a
pilot.

The [424880a9 browser checkpoint](rust-m9-424880a9-browser-checkpoint.md)
records fresh full passes for Unified HTML and Static Chrome on the same
ordinary image. Both verified shipped React assets and exact cleanup.

The [424880a9 migrated Standard Preview](rust-m8-424880-migrated-standard-preview.md)
verified the historical SQLite-to-PostgreSQL upgrade, real imported login,
advancing Chromium video through unchanged React, one origin connection,
and exact normal retirement. Direct copied-SQLite playback remains open.

The [424880a9 Compose checkpoint](rust-m6-m8-424880a9-compose-checkpoint.md)
verified copied Streams account filters, Logo usage filters, and dismiss-all.
The ordinary rehash matched Original. Reply loss returned HTTP 503 on Rust
and HTTP 200 on Original, with the committed frame counts recorded separately.

The [paired playback follow-up](rust-m2-m3-shared-playback-compose-followup.md)
matched finite MPEG-TS handoff on 4558a85b and exercised two-viewer fMP4
playback with Native dynamic health enabled on 25dc1502. Both comparisons
saw one upstream connection at a time and verified exact cleanup.

The [9ace9f66 Compose checkpoint](rust-m6-m8-9ace9f66-compose-checkpoint.md)
matched 36 copied Logo GET and HEAD observations and a normal provider
rehash. The measured broker reply-loss case still differs in first HTTP
status and committed frame count.

The [25dc1502 Core and timeshift follow-up](rust-m9-25dc1502-core-and-m4.md)
adds one qualified exact-source Core component row and records 21 fresh
passes across 127 required rows. The separate numeric seek and calendar
Range replay matched 20 fresh HTTP 206 responses.

The [25dc1502 release rehearsal](rust-m9-25dc1502-formal-and-package-scopes.md)
recorded 20 fresh passes across 127 required rows after the same-image
first-admin browser replay. The first failure remains in the ledger.
The separate HDHomeRun and backup filename comparisons also completed.

The [provider fault and resolver checkpoint](rust-25dc1502-provider-faults-and-resolver.md)
matched all 16 Xtream fallback and recovery cases on 25dc1502. It preserves
the measured e65b4318 broker reply-loss response difference and records 187
Original resolver samples separately from HTTP operation coverage.

The [25dc1502 copied SQLite workflow](rust-m8-25dc1502-copied-streams-dismissal.md)
passed 34 Streams startup reads, 20 Streams Name reads, and five personal
dismissal responses. The exact image preserved notification state after
restart, and both Compose scopes verified cleanup.

The [e65b4318 native ARM64 restart](rust-m10-e65b4318-native-arm64.md) passed
on lappy with a locally generated copied SQLite fixture, protected React
shell and assets, the direct login shell, and five Core reads across both
generations. Exact cleanup passed.

The [fault references and e65b4318 warm cutback](rust-e65b4318-original-faults-and-warm-cutback.md)
record Original's broker reply-loss, Xtream fallback and recovery, and single
notification dismissal behavior. The exact e65b4318 image also passed the
scoped copied PostgreSQL warm cutback with one acknowledged fallback write.

The [e65b4318 Compose checkpoint](rust-m6-m8-e65b4318-compose-checkpoint.md)
matched Original's definite broker refusal and copied SQLite startup reads.
The copied Standard Chromium flow completed real sign-in, rendered its owned
channel, loaded Logos, and verified WebSocket delivery and shutdown.
The administrator HEAD and OPTIONS pairs for both HDHomeRun roots also matched.

The [4558a85b grouped Compose checkpoint](rust-m9-4558a85b-formal-compose.md)
recorded 26 fresh passing workflows across 127 required rows.
The matrix retains each other disposition and the Original reference separately.

The [4558a85b standalone wave](rust-m9-4558a85b-standalone-compose.md)
passed six current-image Compose checks with verified cleanup. Nineteen other
bindings passed preflight and still needed physical execution at this checkpoint.
The remaining matrix entries include 101 blocked rows and one retained Original
reference.

The [f75cac4e formal wave](rust-m9-f75cac4e-formal-and-auth-hash.md) recorded
four passes, one Auth Hash callback failure, 121 blocked nodes and one retained
Original result across 127 required nodes. The Auth Hash workload and later
independent cleanup and semantic reduction succeeded. The original exit 2 and
failed row remain recorded. A fresh 15-cell same-image retry passed preflight.
The earlier `e740306c` wave recorded 26 passes on its own package. Every
milestone still has open acceptance work. The next comparative resource
measurement follows functional acceptance.
[Earlier recorded matrix](../scripts/rust-integration/m9-e740306c-formal-matrix.json)

The [live provider retry](rust-m2-provider-capacity-retry.md) matched the
ordinary full-slot HTTP 503 response. Both backends delivered media after a
normal last-viewer close and a fresh request following slot retirement. An
active-handler overlap was observed on Native during one global-stop repeat.
Three [socket-aware repeats](rust-m2-2c939204-origin-stop-causality.md) each
observed peak one on both backends. The earlier overlap remains unexplained.

The [shared Core repository refresh](rust-m6-core-saved-plugin-refresh.md)
routed a saved plugin schedule through the existing worker and recorded a
single successful result and durable tick. The [Version HTTP 503 pair](rust-m6-a8ea333d-core-version503.md)
qualified the daily failure-response behavior on a8ea333d. The [startup ETA pair](rust-m6-34860-core-plugin-startup-eta.md)
also matched the delayed claim, acknowledgment and result on 34860b9a.
A saved-only control ran once with zero startup publications through readiness.
The [55c895bf provider rehash](rust-m6-55c895-core-provider-rehash.md) matched
the ordinary PATCH, two-stream merge and real worker completion. The
[Original acknowledged held-worker control](rust-m6-55c895-original-rehash-held-worker.md)
showed the saved override and exactly one pending task before replacement
worker startup. The publication-failure cases remain open.

The [native ARM64 copied SQLite preview](rust-m10-3843b61a-native-arm64-copied-sqlite.md)
passed a same-image restart on lappy. The copied database retained the saved
app data, the existing bearer and token refresh. The [AMD64 React shell preview](rust-m8-2c939204-copied-sqlite-react-shell.md)
matched the Original HTML on the root and channels routes and served the
packaged JavaScript and CSS across restart. Channel List JSON and browser
workflow checks remain open. The [published native ARM64 shell](rust-m10-a379-native-arm64-copied-sqlite-shell.md)
then matched all four AMD64 bodies through a restart on lappy. The [34860b9a
Channel List cohort](rust-m8-34860-copied-sqlite-channel-shell.md) matched 13
GET bodies byte for byte and the HEAD status and headers. It repeated ten
administrator calls after restart with the same bearer tokens and served
unchanged React assets. [The native ARM64 Channel List](rust-m10-34860-native-arm64-copied-sqlite-channel-list.md)
also passed a restart on lappy and retained filtering, effective values, and
two ordered stream relationships. The [55c895bf read cohort](rust-m8-55c895bf-copied-sqlite-channel-reads.md)
matched Channel List, detail, IDs and Guide summary. All 29 GET bodies and
three HEAD lengths matched Original, and 20 admin reads repeated after restart.
The build reused the ordinary Cargo layer from the same verified archive.
The [f75cac4e copied SQLite bootstrap cohort](rust-m8-f75cac4e-copied-sqlite-bootstrap-groups.md)
matched 19 Core reads and exercised eight scoped Channel Group operations
across two generations. The [Original copied SQLite bootstrap reference](rust-m8-original-copied-sqlite-bootstrap-72.md)
recorded 72 initial requests and 72 repeated requests after reopening the
adapted historical copy. Its matching Native replay and broader browser
workflows remain pending. Comparative playback performance remains open.
The [55c895bf cached Xtream info](rust-m7-55c895-cached-xc-info.md) also matched
eight paired requests, with unchanged SQL and zero provider calls.
The [Original stale and missing metadata cohort](rust-m7-original-xc-inline-info-refresh.md)
recorded all four responses and eight provider requests without queue changes.
The [4558a85b paired Compose checkpoint](rust-m6-m7-m8-4558a85b-compose-checkpoint.md)
matched those four Xtream refresh cases in the ordinary image. Its copied
SQLite cohort completed 144 serial observations and two ten-request
bootstrap bursts. The setup-status Vary discrepancy and the Native
rehash refusal response and lock behavior remain open.

The [55c895bf archive cohort](rust-m4-55c895bf-archive-interop.md) matched a
same-session programme hop and a transformed XC archive range with useful
audio and video. Both paired runs verified upstream and provider-slot
accounting and exact teardown. Broader M4 fault and resource checks remain
open.

The [2c939204 ordinary playback batch](rust-m3-2c939204-ordinary-output-cohort.md)
passed shared HLS, shared profiled fMP4, live fMP4 lease renewal and natural
raw MPEG-TS failover. Each pair verified teardown. Broader output-process
recovery and the comparative resource gate remain open.

The [shared Core restore](rust-m8-core-postgres-restore.md) restored an
Original PostgreSQL archive and a Rust recovery archive in order, with exact
scoped SQL, fresh login and settings/channel readback. The same image kept
an active `pg_restore` process through SIGTERM until its signed task settled.
The guarded restore remains an opt-in path for the current database schema.

The [shared Core scheduler](rust-m6-m8-shared-daily-jobs-and-backup-replay.md)
passed fresh paired account-expiration and Version checks. Its two scheduled
backup interruption tests matched the Original outcomes across Redis and
PostgreSQL, with one durable tick in each case. Default worker coverage and
full restore acceptance remain open.

The [live MPEG-TS stop and retune check](rust-m2-e9d143b8-contained-stop-retune.md)
verified authenticated cross-worker stop, opposite-worker retune, and useful
audio and video. Concurrent viewers reused one upstream and one provider slot
per backend in the scoped comparison.

The [native ARM64 Docker Desktop package](rust-m10-e9d143b8-native-arm64-docker-desktop.md)
started locally with PostgreSQL, Redis and unchanged React. The same image
passed first-admin setup, sign-in and protected group listing. The earlier
[d0a80aa5 startup](rust-m10-d0a80aa5-native-arm64-docker-desktop.md) and
[first-run auth](rust-m10-d0a80aa5-native-arm64-first-run-auth.md) retain their
own package evidence. Controlled rollout awaits complete M9 evidence.

The [81ce Guide renderer package](rust-m7-browsable-guide-original.md#packaged-rust-successful-list-get)
matched the successful HTML list and its cookie behavior, passed all 37
programme JSON and HEAD comparisons, and passed the 18-suite packaged
baseline. The e765 coordinator results retain their exact image binding.

The [complete parity plan](rust-complete-parity-plan.md) assigns parallel work
across M0–M9 and proposes M10/M11 as separate rollout stages.

The [current network-disabled SDK](../scripts/rust-contracts/sdk-live-typed250.json)
passed all 119 tests, reproduced the register byte-for-byte and verified cleanup.
The register has partial Original coverage for 797 of 798 HTTP declarations
and all 250 typed declarations. The new [registration evidence](rust-m0-migration-source-evidence.md)
covers 20 Celery tasks and three Django management commands in the pinned
Original runtime. The earlier [113-case SDK](../scripts/rust-contracts/sdk-live-typed227.json)
covers the prior registry. Full role and Native parity remain open.

The [Original packaged React root](rust-m0-original-aio-root.md) returned
HTTP 200 with the exact baked index and both linked assets under the explicit
disposable OS login locale fixture. The deployed resolver and exact teardown
also passed. The current SDK binds that capture to the Original anonymous GET
as partial coverage.

The [Redis write retry and shared playback checks](rust-migration-shared-playback-checks.md)
verified the same-writer retry after a rejected write, the joined transport
switch, and two equivalent fMP4 viewers sharing one upstream. The paired HLS
screen passed with 33.30% less CPU and 47.18% less peak working memory.
The [cross-replica fMP4 check](rust-m3-live-format-continuation.md) also passed
with one upstream and one provider slot. The initial 100-viewer comparison
missed the equal-work bound. The [anchored diagnostic](rust-m2-100-viewer-anchor.md)
then met that bound on both implementations. The [fresh 100-viewer
comparison](rust-m2-100-viewer-measured.md) passed all three pairs with matched
work. Rust used about 83% less CPU and 71% less peak working memory. The
[shared-image repeat](rust-m2-100-viewer-measured.md#shared-package-repeat)
passed three additional equivalent-work pairs. Median paired CPU and peak
working memory fell 82.6% and 71.0%, respectively.

The [ordinary Main VLC check](rust-m3-ordinary-vlc-playback.md) verified two
authenticated MPEG-TS readers sharing one VLC input, upstream connection
and provider slot on the exact shared package. Both readers decoded video
and audio, and Main stopped cleanly with exact owned cleanup.

The [Candidate107 VOD and XMLTV results](rust-milestone-batch107.md) verify
calendar seek ranges, alternate-provider capacity and remote source creation.

The [e765 VOD and catch-up Range pair](rust-m4-e765-ordinary-seek-calendar.md)
matched 20 authenticated HTTP 206 responses and one upstream request per case
and backend. All eight Compose roles retired and both projects were empty.

The [8241 VOD and catch-up Range pair](rust-m4-8241-ordinary-seek-calendar.md)
verified the ten ordinary cases again on this exact image. All 20
authenticated responses returned HTTP 206 with matching bodies, one
upstream request per backend per case and complete project cleanup.
The [Candidate107 resource comparison](rust-m4-performance.md#candidate107-playback-comparison)
passed 12 VOD and catch-up pairs with matched work. Median paired CPU fell
39–73% and working memory fell 32–36% depending on the workload.
The [Candidate108 API results](rust-milestone-batch108.md) verify Core JSON
routes, the stock React channel-guide edit and legacy backup schedule reads.

The [opt-in playlist Refresh All workflow](rust-m6-refresh-all-original-worker.md)
passed on both the standalone and integrated packages. One retained Original
Celery consumer completed the aggregate job and both children, with the
expected streams and group memberships. The ordinary Native worker graph
and equivalent-work measurement remain open. The
[Original stock Groups capture](rust-m6-remote-std-stock-groups.md) also
verified account detail and group-list output against the imported rows.

The [explicit VOD session check](rust-vod-explicit-session-id.md) passed
four paired cohorts. Same-ID requests reused their sessions and fresh
Native IDs had their own stats rows. Original returned 503 in both
early-close fresh-ID cases. Both admitted a fresh ID once the old row
was absent and a provider slot was free. Provider-slot release timing
remains open.

The [Native managed-log check](rust-m8-native-managed-logs.md) verified
startup-file production, authenticated list and tail, and clean process
shutdown. Focused Rust controls cover failed rotation and standby ownership.
The same package passed 29 paired stored System Logs cases on one
read-only mount, with exact responses, unchanged files and clean teardown.
A paired stock Settings change lowered retention from five to two and
pruned the expected archives while keeping the active log readable.
Scoped persistence toggles verified that ordinary console output continues
while file writes are disabled and that the next restart files its normal
startup record after re-enabling persistence.

The [stock backup missing-settings replay](rust-m8-backup-missing-row.md)
matched permissions, response bytes, row creation and cache invalidation
when disabling the schedule. Both owned fixtures retired cleanly.

The [standalone manual PostgreSQL backup](rust-m8-standalone-backup-create.md)
matched Original create, signed task status and archive download on the
exact a785 package under separate owned namespaces. The opt-in consumer
verified normal shutdown and exact cleanup in both Compose projects.

The [stock Stream multipart replay](rust-m5-stream-file-compose.md) matched
all 16 paired cases on the shared image. The
[UTC time-only custom guide replay](rust-m7-utc-guide-compose.md) passed both
phases on XMLTV, the programme grid, and Xtream short EPG. Both respective
Compose projects retired exactly.

The [C74 package checks](rust-c74-clean-install-operator.md) passed nine
distinct required nodes across six runs on the same image. The checks
covered 11 stock React fresh-install, 17 safe operator, 22 shipped
`createsuperuser`, 154 strict API, 64 current-programme and 43 guide-grid
cases. The production two-worker MPEG-TS node passed all 24 checks. Two
further nodes passed 14 restart checks and the functional retained-history
fixture with 100 late readers and 20 audio/video decodes. The full
integrated package and resource gates remain open.

The [e852 permissions and management baseline](rust-e852-permissions-management-baseline.md) ran all 18 suites.
It recorded 1,023 passing checks and one response-body mismatch for a
malformed settings ID. Cleanup passed. The coordinator exited 2 with
this node failed. The nine C74 passes remain scoped to C74.

The [735 persisted password upgrade](rust-735-auth-hash-acceptance.md) passed 34 checks and
verified cleanup on 735, covering eight persisted credential variants.

The [a785 persisted password upgrade](rust-a785-auth-hash-acceptance.md) passed 34 checks across eight persisted credential variants against a fresh
Original fixture and independently verified Compose cleanup.

The [a785 configured proxy identity and access policies](rust-a785-proxy-auth-acceptance.md)
passed 38 checks in a paired Original and Rust Compose fixture, including
20 configured identity and access-policy checks, with verified cleanup.

The [735 createsuperuser and typed comparisons](rust-735-cli-acceptance.md) passed 22 shipped
createsuperuser cases and 154 typed comparisons on 735. The coordinator
recorded two passing nodes and verified cleanup.

The [a785 createsuperuser and typed comparisons](rust-a785-cli-acceptance.md)
passed 22 shipped CLI cases and 154 typed comparisons with verified cleanup.

The [976f createsuperuser and typed comparisons](rust-976f-cli-acceptance.md)
passed 22 shipped cases and 154 typed comparisons, with verified cleanup.

The [735 operator and guide acceptance](rust-735-group-acceptance.md) passed four required nodes
on 735. It verified 11 clean-install checks, 17 safe CLI cases, 64 current
programme cases and 43 guide grid cases with verified cleanup.

The [a785 operator and guide acceptance](rust-a785-group-acceptance.md) verified 11 clean-install checks, 17 CLI cases, 64 current programme cases
and 43 guide grid cases with independent Compose cleanup.

The [976f operator, guide and authentication acceptance](rust-976f-group-acceptance.md)
passed six required nodes covering installation, operator commands, guide
reads and authentication with independent Compose cleanup.

Four scoped Compose runs now verify 12 distinct required coordinator nodes
on `976f2867`, with 115 of 127 still unqualified for this exact package.
The remaining count includes the retained Original dependency, which served
as reference in the operator run. Full release acceptance and its resource
comparison remain open. [Combined scope evidence](../scripts/rust-integration/milestone-batch113/976f-current-required-node-coverage.json)

The [735 permissions and management baseline](rust-735-baseline-acceptance.md) passed all 1,024 checks
across 18 suites with verified cleanup. It matched the exact HTTP 404
body for the malformed settings ID. The coordinator recorded one passing
node on 735.

The [a785 permissions and management baseline](rust-a785-baseline-acceptance.md) passed
1,024 checks across 18 suites on the standalone-backup successor image. The
run decoded 75 video frames and independently verified project cleanup.

The [976f permissions and management baseline](rust-976f-baseline-acceptance.md)
passed 1,024 checks across 18 suites on the standalone-restore successor
image, decoded 75 video frames and independently verified project cleanup.

The [e765 permissions and management baseline](rust-e765-baseline-acceptance.md)
passed 1,024 checks across 18 suites on the backup schedule request successor
image, decoded 75 video frames and independently verified project cleanup.

The [976f scheduler setup and daily Version run](rust-m6-976f-scheduler-startup.md)
observed six stock Beat rows and one actual Version result per backend, with
selected Main readiness and independent cleanup. The wider M6 graph remains open.

The [e765 ordinary channel-maintenance pair](rust-m6-e765-ordinary-channel-tasks.md)
matched four authenticated job outcomes through HTTP, Redis, Channels and SQL
with exact cleanup. The first controller exit 1 remains recorded separately.

The [e765 scheduled XMLTV run](rust-m6-e765-scheduled-xmltv.md) verified one
due refresh per backend, both settled descendants, programme and index
effects, and exact cleanup on the same packaged candidate.

The [Original aggregate XMLTV run](rust-m6-original-aggregate-xmltv.md) verified
ordered inline refresh for two active sources, four result-linked descendants
and exact cleanup. The [paired Rust aggregate and individual XMLTV run](rust-m6-d482-aggregate-xmltv.md)
matched that workflow and the independent single-source refresh on one exact package.
The wider M6 gate remains open.

The [Original compatibility Beat wrapper](rust-m6-original-compat-beat.md) verified its inline Stats and scanner effects, the linked group child and exact retirement.

The [paired compatibility Beat wrapper](rust-m6-c2a3-compat-beat.md) matched the Original Stats, scanner and linked group child effects on the exact packaged Rust Main.

The [enabled recurring recording pair](rust-m6-c2a3-enabled-recurring.md) verified the hourly UTC rule, future clocked recording, notification and completed artwork child.

The [735 MPEG-TS functional checks](rust-m9-735-functional-streaming.md) passed
all three required nodes with verified cleanup. The run covered 24 two-worker
checks, 14 delayed restart checks and 100 retained-history readers with 20
audio and video decodes in the history fixture.

The [stock React Group Manager retry](rust-m5-stock-group-manager.md) passed
ordinary Native login, cancel and both cleanup modes with exact SQL and
teardown. The paired Original HTTP reference was retained.
The [stock React provider modal replay](rust-provider-modal-browser.md)
matched all 12 scoped image requests and decodes across both Chromium phases.

The [e765 React live playback and Stats gate](rust-e765-react-live-stats.md)
verified real Chromium login, decoded advancing video, active-channel Stats
and normal player close on both backends, with exact owned cleanup.

The [114558 React live playback and Stats run](rust-114558-react-live-stats.md)
repeated the two browser arms on the exact Guide successor package and
verified all 112 shipped React assets and independent process and Compose cleanup.

The [c606 React live playback and Stats run](rust-c606-react-live-stats.md)
repeated the same two browser arms on the newer exact package. Both decoded
live MPEG-TS, returned the active channel in Stats, and closed playback through
the UI. All 112 shipped React asset hashes and independent teardown matched.

The [trusted plugin action replay](rust-m8-selected-trusted-plugin-actions.md)
matched the decoded results and selected headers for echo and scalar with
selected workers and verified exact compatibility-host retirement.

[The late Redis commit replay](../scripts/rust-core-contracts/late-commit-replay/receipt.json)
preserved all 23 paired streaming core cases. A fresh Rust writer recovered
at the physically committed index and delivered both ordered payloads before
releasing its viewer, local cache and scoped Redis keys.

The [scoped PostgreSQL cutover and rollback](../scripts/rust-integration/cutover-candidate87-scoped.json)
used copied data, the same loopback URL and authenticated session, and one
fixture edit confined to the copy. The Original channel, media, schema and
sequences matched after rollback. Its full SQL fingerprint matched immediately
before restart and differed afterward. The first failed observer attempt
and both cleanup receipts are retained. Whole-package soak and the release
decision remain open.

The [instrumented rollback replay](rust-cutover-candidate87-row-attribution.md)
found zero Original row changes during Native use. A standalone Original
restart reproduced the same four timestamp updates in the paired replay.
The strict post-restart fingerprint still differed.
The [Candidate110 and Candidate111 rehearsals](rust-m9-copied-postgres-rollback.md)
reproduced that result. The copied-data cutover and same-session rollback
worked, and both strict gates recorded the four Original startup writes.
The [same-seed C74 pair](rust-m9-copied-postgres-rollback.md#c74-same-seed-control-and-copied-cutover)
isolated the Native edit and verified all other keyed Original fields
stayed equal. The four startup timestamps still produced strict exit 1.

The [120-second playback and guide diagnostic](rust-m9-playback-refresh-diagnostic.md)
passed on Candidate91 with ten viewers sharing one owned upstream and
20 decoded clips. A second trial on Candidate94 passed with the longer
H.264/AAC origin and two distinct XMLTV jobs during ten-viewer playback.
A third resource trial passed the full 120 seconds with the one-slot provider,
complete resource censuses and exact cleanup. The
[Candidate110 two-hour soak](rust-m9-guide-lock-diagnostic.md#two-hour-playback-and-refresh-soak)
passed with ten viewers, one upstream and provider slot, twelve completed
refreshes, and 140 audio/video decodes. The integrated M9 release gate remains
open.

The [five-minute password regression](rust-m9-playback-refresh-diagnostic.md)
passed on Candidate103 with zero and ten viewers. The viewing cohort
preserved one upstream and its provider slot during login. The fourth
stationary login still returned HTTP 429 with no throttle-history change.

The [completed DVR performance attempt](../scripts/rust-perf/candidate88-dvr/README.md)
verified three one-viewer pairs. Median paired whole-stack CPU fell 45.05%
and peak working set fell 62.99% with equal delivery. Native returned
six HTTP 503 responses in the first ten-viewer warmup, and the controller
exited 1 after exact cleanup.

The [HDHomeRun JSON-suffix checks](rust-m7-hdhr-json-writes.md) verified 32
ordinary Native device writes and denials on the pinned Candidate111 image.
The [device XML and output-profile status comparison](rust-m7-hdhr-device-xml-output-status.md)
matched six full response digests and selected headers per backend on e852,
with unchanged scoped SQL and exact Compose cleanup.
The register now includes the 16 [Original DRF root operations](rust-m0-original-drf-roots.md).

The [development and validation plan](rust-development-and-validation-plan.md) defines parallel implementation/acceptance/tooling lanes, affected-workflow batches and default-off fixture concurrency. Performance measurement remains exclusive.

The [f960875 three-project Compose run](rust-m9-f960875-parallel-compose.md) passed three existing functional rows and exact cleanup on one host. The observer captured 40 simultaneous project samples and 14 of 15 configured port bindings. The strict overlap check remains open.

The [XC account lifecycle Source capture](../scripts/rust-integration/milestone-batch67/xc-source11-checkpoint.json)
completed all 12 calls in eleven cases, with controller exit 0 and verified
cleanup. The retained capture review accounts for observed Redis queue pruning
and expiry, qualifying all 12 calls without replay. It records pre-setup create
responses, real profile task lineage and the category rows retained on account
deletion. The Source business review passes all eleven cases. Rust parity
remains in progress.

The [channel rename and Auto Sync checkpoint](rust-milestone-batch102.md)
matched the long-name PostgreSQL cast and both ordinary and wide regex
preview suites. The [release benchmark and repeat](../scripts/rust-perf/candidate102-regex-preview/README.md)
measured lower CPU and peak working set at equal throughput. The same
two full-scan p95 pairs missed the limit in both attempts. The shifted
timing experiment did not reproduce the spikes, and the isolated sample
allocation change still missed the release limit.

The [enabled DVR rebuild checkpoint](rust-milestone-batch104.md) matched
the selected non-UTC overnight edits and activation, scheduled rows and
artwork children. The numeric VOD seek fixture stopped before an Original
HTTP observation and awaits replay.

[The viewing and client compatibility checkpoint](rust-milestone-batch100.md)
records the exact Docker build, recurring DVR detail and VOD stats parity,
and two-replica TS controls. Targeted client stop disconnected just the
selected viewer while its peer continued through the shared upstream.

The [network-disabled SDK](../scripts/rust-contracts/sdk-image57-core-json-530.json)
reproduced the 530-assignment HTTP register and passed all 22 tests,
including separate validation of the 19 corruption controls. The owned
container ran with network mode `none` and exact teardown. The register
has 268 unassigned HTTP operations and zero fully verified HTTP operations.
The [earlier run](../scripts/rust-contracts/sdk-image57-ingress-517.json)
preserves the 517-assignment result.

[The programme mutation and admin redirect checkpoint](rust-milestone-batch99.md)
records the configured admin Compose pass, paired gzip guide recovery, and
separate 16-operation Original management capture. The paired Programme
mutation package passed all 12 requests per backend.

[The guide clear, HDHomeRun and XC alias checkpoint](rust-milestone-batch98.md)
records three passed Compose scopes on one image. The Original public admin
redirect was captured, and both backends served the literal `admin` XC alias.

[The XC live-to-VOD checkpoint](rust-milestone-batch95.md)
verifies three direct pairs and records the timed-out Candidate95 scheduled
case. Candidate96 passed its focused tests and the exact scheduled Compose
replay. Both backends completed the live refresh and its VOD child with
matching task lineage and owned records.

[The batch guide and Auto Sync checkpoint](rust-milestone-batch94.md)
passes selected XMLTV and source-less programme publication, the default
atomic guard, and the exact packaged React regex preview contract. The paired
Auto Sync benchmark measured 16.72% lower CPU and 56.36% lower peak working set,
with two full-scan p95 failures.
[Paired Auto Sync benchmark](../scripts/rust-perf/candidate94-regex-preview/README.md)
The follow-up batching correction passed all 48 exact Compose checks.
The repeat benchmark measured 18.08% lower CPU and 55.42% lower peak
working set with equal work, with two full-scan p95 failures.
[Paired batching replay](../scripts/rust-perf/candidate97-regex-preview/README.md)

[The persisted System Logs checkpoint](rust-milestone-batch92.md)
verifies metadata, cursor tail and attachments against the same read-only
file set in both packaged backends.

[The batch-guide and DVR writer checkpoint](rust-milestone-batch91.md)
passes both exact paired Compose fixtures. Its [completed-recording benchmark](../scripts/rust-perf/candidate91-dvr/README.md)
passed three pairs per viewer level under the same workload and budgets.
Median paired CPU and peak working set fell 41.75% and 64.34% at one viewer,
and 64.62% and 58.88% at ten viewers. Both backends delivered identical bytes.

[The saved Connect Test checkpoint](rust-milestone-batch89.md)
records the Original webhook action and the tested Native port.

[The recording playback and legacy HLS checkpoint](rust-milestone-batch88.md)
documents ordinary DVR reads, old playlist descriptors and their focused checks.

[The guide publisher and PostgreSQL defaults checkpoint](rust-milestone-batch87.md)
documents the refresh-only programme publisher and blank operator settings.

[The selected-worker readiness checkpoint](rust-milestone-batch86.md)
covers Celery and DVR first progress and the pending packaged startup check.

[The channel guide and modular PostgreSQL TLS checkpoint](rust-milestone-batch85.md)
records Set EPG for the existing React channel editor and the certificate-only
connection settings for Main and its database operator.

[The XC capacity and priority checkpoint](rust-milestone-batch84.md)
records the next stock account edit and its checks.

[The integration deletion and PostgreSQL credential checkpoint](rust-milestone-batch83.md)
records the next Rust changes and their validation.

[The integration-edit, XC daily schedule and Redis startup checkpoint](rust-milestone-batch82.md)
records the next Rust fixes and retained failover media controls.

[The subscription replacement and XC interval checkpoint](rust-milestone-batch81.md)
records the next Rust implementation and its verification.

[The integration-create and XC rename checkpoint](rust-milestone-batch80.md)
records the next Rust fixes and their focused verification.

[The Delivery Logs, TS response and Redis TLS checkpoint](rust-milestone-batch79.md)
records the next Rust fixes and the full Store library test result.

[The Connect collection and Redis configuration checkpoint](rust-milestone-batch78.md)
records the next Rust fixes and their focused checks.

[The account activity, HLS profile and local-playlist checkpoint](rust-milestone-batch77.md)
records the next focused Rust fixes and upcoming packaged comparisons.

[The XC orphan cleanup and archive fault checkpoint](rust-milestone-batch76.md)
records eleven Store tests and eight real-Redis media tests. The earlier
[guide, XC, archive and operator checkpoint](rust-milestone-batch75.md)
records the exact-image Compose results for those four workloads.

The [retained Original playback packet](../scripts/rust-contracts/retained-playback/README.md)
binds 54 observed HTTP responses to seven additional route-operation entries.
The [retained Image22 controls](../scripts/rust-contracts/retained-image22-controls/README.md)
add 22 responses across seven channel and transport operations. The
[repository, socket and switch packet](../scripts/rust-contracts/retained-repository-socket-switch/README.md)
adds 57 retained observations across six unresolved declarations. The
[Original admin capture](../scripts/rust-contracts/retained-admin-models/README.md)
adds 283 model responses across 103 previously unmapped operations. Its
controller exited 1 and exact cleanup passed. Failed expectations and
blocked cases remain visible in the receipt. M0 now has 373 partial Source
assignments and 425 unassigned entries after the admin capture. The
[retained DVR and XC packet](../scripts/rust-contracts/retained-dvr-xc/README.md)
adds 49 responses across five operations. The
[retained Connect collection packet](../scripts/rust-contracts/retained-connect/README.md)
adds 24 Original responses across the separate GET and POST operations.
The [retained Connect actions](../scripts/rust-contracts/retained-connect-actions/README.md)
add 14 delivery-log GET observations and 13 literal subscription replacement
PUT observations. Four additional successful PUT responses are preserved
using their fixture symbols. The [retained alias capture](../scripts/rust-contracts/retained-alias19/README.md)
adds 19 anonymous Original responses with verified resolver and observer
evidence. Its controller exited 1 after reaching the fixture log limit.
The twentieth response remains outside the assignments because its
post-response evidence is incomplete. The [retained Connect detail results](../scripts/rust-contracts/retained-connect-detail/README.md)
add 14 literal PUT and seven literal DELETE responses. Six successful responses
retain their symbolic fixture paths and database observations. The
[alias remainder](../scripts/rust-contracts/retained-alias-remainder/README.md)
adds 64 partial assignments and supplements the 19 prior anonymous primaries.
It retains the 75 observed HTTP 500 outcomes. The [Connect saved webhook Test binding](../scripts/rust-contracts/retained-connect-test/README.md)
adds one partial Source assignment with eight matched HTTP observations and
an owned receiver count check. The [single Set EPG binding](../scripts/rust-contracts/retained-set-epg-selected/bindings.json)
and [batch Set EPG binding](../scripts/rust-contracts/retained-set-epg-batch/bindings.json)
add two partial assignments for the unformatted operations. M0 now has
470 partial HTTP assignments and 328 unassigned operations.
The conservative method inventory and full acceptance requirements remain
in the [contract register](rust-contract-register.md).

The [previous Rust checkpoint](rust-milestone-batch74.md) covers concurrent
archive setup, XC group validation, packaged multipart guide requests and
standalone PostgreSQL cross-restore.

The [Candidate73 checkpoint](rust-milestone-batch73.md) covers the stored
archive route and retained M3U, channel, VOD and HDHomeRun observations. The
register has 250 partial Source assignments and 548 unassigned references.

The [Candidate72 checkpoint](rust-milestone-batch72.md) covers archive
fingerprint handoff, first-tick Beat readiness and new retained serializer
contracts. M0 has 211 partial Source assignments and 587 unassigned HTTP
references.

The [Candidate71 checkpoint](rust-milestone-batch71.md) covers fenced archive
reopening, XC membership and refresh behavior, custom-guide timezone dates,
operator arguments, signed backup downloads and the packaged fixed-seed EPG
comparison. M0 has 188 partial Source assignments and 610 unassigned HTTP
references after the retained Core GET review.

The [previous parallel checkpoint](rust-milestone-batch70.md) covers typed HLS
startup, guide multipart handling, fixed-seed XC EPG, operator arguments and
signed backup filenames. The pinned Original all-in-one entrypoint selected
PostgreSQL. SQLite compatibility still needs a legitimate Source fixture. M0 now
assigns six additional guide GET routes from the retained stored-EPG run, for
181 partial assignments and 617 unassigned HTTP references.

The [parallel development checkpoint](rust-milestone-batch69.md) groups playback
TLS, channel-range and M3U behavior, XC cache and WebSocket behavior, backup
filenames, and release workflow changes. Focused development checks pass.
The full acceptance run is reserved for the integrated release candidate.

[Candidate68](rust-milestone-batch68.md) adds bounded XC account creation and
category-only account deletion. The first Native test found a profile-worker
TLS trust mismatch and exited after four calls. The corrected snapshot passes
all SDK117 gates, including 1,240 workspace tests, and its Docker release build.
The corrected Native run exits 0 after all twelve HTTP calls and all five task
results match. Literal wire and state differences remain under review.

[Candidate67](rust-milestone-batch67.md) passes all four SDK115 gates (1,240 passed, zero failed, 802 ignored), its exact-revision Docker build, six runtime package families and three installed dependency lifecycle tests. Ten new Source preview/create observations are captured and reviewed. The affected ten-context Native batch completes with actual exit1: four passed/six failed, with eleven-container/ten-generation cleanup. Guide, logos and live/VOD pass; import, scope and version-observer failures are retained. The three-context retry exits 0, all ten new HTTP comparisons pass, and 23 retained calls qualify through the corrected observer. This covers the selected contexts. Both backends pass the fresh 100-viewer protocol pilot. The [24-phase comparison](rust-candidate67-viewing-comparison.md) passes all twelve scoped live-viewing budget pairs. Broader performance acceptance and full milestone closure remain open.

[Candidate66](rust-milestone-batch66.md) corrects the Source-observed channel-range response headers in one file. SDK112 passes all four gates: 1,238 passed, zero failed, 802 ignored. All fourteen physical Redis/HTTP controls and both strict prefix decodes pass. Its exact-revision cached recovery build and all six package families pass, with the paused original build handle limitation preserved. The twenty-five-context workflow completed with actual exit1:22 passed/3failed; all Comskip cases pass, while range tie-order observation, logo Vary and version fixture permissions require scoped followup. Thirty-container/29-generation cleanup and input integrity verify; full application acceptance remains pending. All complete milestones and current performance acceptance remain open.

[Candidate65](rust-milestone-batch65.md) adds channel-range lookup, enabled-plugin logos and pinned PEP440 version comparison in fifteen scoped files. SDK111 passes all four development gates: 1,238 passed, zero failed, 802 ignored. Its exact-revision Docker build passes in 390.753 seconds; all fourteen physical Redis/HTTP controls and two strict prefix decodes pass. All six runtime package families have passing evidence, retaining the original regex observer failure and its separate successful retry; all 65 owned containers are absent. The ten new Source range observations are captured and reviewed; their Allow/Vary headers expose a small production fix required before expanded Native acceptance. New HTTP and whole-task application acceptance remain pending. These implementation checks close no full milestone or performance criterion.

[Candidate64](rust-milestone-batch64.md) passes all four SDK109 gates (1,236 passed, zero failed, 802 ignored), fourteen actual Redis/HTTP controls including the inactive-profile timer regression, its exact-revision Docker build and all six package families. The Source12 reference qualifies nine SD search and three direct-M3U captures. The source-contract register has 174 partial HTTP assignments and 624 absent, with its portable 358-test gate passing and one optional skip. Native22 has completed with actual exit 1: 20 contexts passed and two observer failures remain recorded. All twenty-two scoped business contexts qualify through preserved captures and narrow offline observer corrections, including the required full own-EDL audio/video content comparison. All 10,560 bound inputs and 28-container/27-generation cleanup verify. Complete milestones and new performance gains remain open.

[Candidate63](rust-milestone-batch63.md) passes all four SDK107 gates (1,232 passed, zero failed, 802 ignored), fourteen explicitly executed Redis/HTTP controls, its exact-revision Docker build and all six runtime package families. It joins the fixes identified by Candidate62 Native19. The completed twenty-context application batch exits1 with sixteen passed and four failed; 8,255 owner inputs and 26-container/25-generation cleanup verify. The failures are isolated to a backup marker no-op divergence, stale VOD idle timer, new SD fixture setup and natural CUT publisher-read transport. Current performance remains unaccepted. The current register has 163 partial HTTP Source assignments and 635 absent assignments; its portable 350-test gate passes with one optional skip. All complete M0–M9 milestones remain open.

[Candidate62](rust-milestone-batch62.md) passes all four SDK103 gates (1,216 passed, zero failed, 802 ignored), thirteen explicitly executed Compose Redis controls, its exact-revision Docker build and all six package families. The complete nineteen-context Native workflow exits1 with fifteen passed and four failed; all 3,844 bound inputs and 24-container/23-generation cleanup verify. Its guide25 regression passes. Provider/category ordering, VOD worker identity/activation, SD headers/observation and natural CUT recording admission remain required followups. An earlier UID-only permission refusal is preserved separately. The batch includes corrected credential observation, channel/guide composition, provider logo allocation, retained VOD adoption and Source-qualified SD actions. The contract register has 161 scoped HTTP Source assignments and fifteen partial non-HTTP candidates; 637 HTTP assignments remain absent. All complete M0–M9 milestones, current application performance gains, soak and rollback acceptance remain open. Candidate60's whole17 run was interrupted by a host reboot, with no observed terminal exit.

[Candidate60](rust-milestone-batch60.md) passes all four SDK100 gates (1,216 passed, zero failed, 792 ignored), all nine actual Redis controls, its exact-revision Docker build, and all six package families. It joins due/immediate recording producer readiness with Source-characterized channel/guide writes and literal sequence corrections. The expanded seventeen-context application workflow remains pending; complete M0–M9 acceptance and new performance gains remain open.

[Candidate59](rust-milestone-batch59.md) passes SDK97 (1,214 passed, zero failed, 792 ignored), all nine actual Redis controls, an exact-revision Docker build, and all six package families. Its full application rerun remains held for a separately identified due/immediate producer readiness risk. The next combined candidate includes that repair and the [new Source channel/guide reference](rust-channel-guide-source25.md): 25 new HTTP observations, six actual programme tasks, literal sequence allocation and the cached-XML deletion quirk. These scoped results close no full milestone or new performance criterion.

[Candidate58](rust-milestone-batch58.md) passes all four SDK96 gates (1,214 passed, zero failed, 789 ignored) and builds from its exact published six-file Rust repair. It adds the inactive STD root consumer, Source-characterized bounded provider lifecycle domains and missing provider redirect metadata. All six package families pass with 55 exact containers independently absent. The matched twelve-call management workflow passes on Candidate58 with 2,084 unchanged inputs and verified four-container/three-generation cleanup; the expanded sixteen-context application batch is held for the known automatic CUT booking503 pending a repaired successor. The [ten-root Source provider reference](rust-provider-lifecycle-source10.md) is qualified separately; no new performance gain or complete milestone passes.

[Candidate57](rust-milestone-batch57.md) implements the bounded positive account/profile/EPG detail lifecycle domains and safe read-only preflight for idle-closed result connections. SDK95 passes all four gates (1,208 passed, zero failed, 789 ignored), its exact-source Docker build passes, all six package families pass, and all six real-Redis reconnect controls pass. The first twelve-call Native management workflow exits 1 after account creation returns 201: its generated unparented STD refresh task is not admitted by the existing parent-only consumer. Cleanup and all 2,079 input hashes verify. Its completed fifteen-context regression exits1: thirteen contexts pass, while credential response-header comparison and automatic CUT booking503 fail. Positive manual Comskip and natural automatic MARK, including terminal results, pass; whole natural media equivalence remains unclaimed. The matched management rerun remains pending. These bounded implementations do not establish all fields or mutation domains, and no new performance gain or complete milestone passes.

[Candidate56](rust-milestone-batch56.md) adds bounded DVR result-transport diagnostics, preserving timeout and retry behavior. SDK90, its Docker build and all six package families pass. Its focused long-recording reproduction exits 1 and records an immediate physical Redis disconnect on the parent result write; read-only inspection finds the Source-configured 300-second idle timeout. Candidate57 addresses that transport failure without retrying the result mutation or changing the global idle timeout. The original-application management reference has four separately qualified calls from an exit-1 capture plus eight observed calls from a successful explicit-checkpoint continuation; it remains two controllers, without whole-cache continuity proof.

[Candidate55](rust-milestone-batch55.md) implements nested provider-profile PUT/PATCH and notification effects. SDK89 passes all four gates (1,198 tests), all 1,451 delivered Rust-tree inputs match the published commit, its Docker build passes, all six package families pass, and the targeted PostgreSQL resolver regression passes. Whole Native application acceptance and new performance gains remain pending. The [confirmed remaining implementation batches](rust-candidate54-remaining-implementation.md) distinguish nine other management mutation bindings from conditional features and untested milestone requirements.

[Candidate54](rust-milestone-batch54.md) passes SDK86 (1,193 tests), the targeted PostgreSQL credential-resolver gate, its Docker build and all six package gates. Its frozen metadata rerun passes all twelve requests with 463 unchanged bound inputs and independently verified cleanup. The initial fifteen-context run was interrupted. Its completed recovery batch exits 1 with thirteen contexts passed and two failed: profile PUT returns 501, and automatic MARK lacks parent/child terminal results despite completed media effects. The observer and token-expiry repairs work; automatic CUT remains unrun. [Recovery evidence](rust-milestone-batch54-recovery.md) distinguishes those results and the pre-startup helper-path failure. No new performance gain or complete milestone acceptance follows.

Candidate53's [Native plugin metadata transport batch](../scripts/rust-integration/plugin-repository-transport-native1/index.json)
captures all twelve requests but exits 1 in 130.630 seconds. Strict comparison
finds a startup notification timestamp that fails to advance. An offline
diagnostic then isolates response Allow/Vary and transport-header differences;
other projected business fields match. Root verifies six exact containers, five
captured host generations and the project absent. These differences remain
required fixes; this capture grants no plugin or milestone acceptance.
Its [original-application reference](../scripts/rust-integration/plugin-repository-transport-source1/index.json)
passes all twelve HTTPS/DNS metadata cases in 134.623 seconds. Twenty-five SQL
snapshots retain all seven selected sequences and all business fields; only
300 account credential values and eleven Authorization fields are redacted.
Earlier bootstrap failures reached zero metadata cases and remain recorded.
Plugin installation, actions and signature execution are separate open scopes.

Candidate52's [second fresh 24-phase viewing campaign](../scripts/rust-integration/milestone-batch52/observer-performance-attempt2/index.json)
exits 1 after 24 attempts and 23 valid phases. The last Native 100-viewer trial 3
fails the unchanged zero-swap rule in external orchestrator/observer scopes;
all service groups have zero swap/OOM and all 100 individual readers pass.
Root verifies 84 exact containers and 24 projects absent. No valid subset,
pair or gain is accepted. The [first campaign](rust-milestone-batch52.md#fresh-viewing-performance-campaign)
and successful 100-viewer protocol pilot remain separate; no old phase is pooled.
All complete milestones and active-work performance acceptance remain open.

[Combined live/VOD settings reference](rust-milestone-batch53.md) qualifies twelve new admin PATCH/account-GET cells: one retained combined save checked offline after controller1, then eleven new cells under controller0 with an exact declared fifteen-table/fifteen-sequence import. Full rows/errors and all sequence effects are retained with only232 login-field redactions. Deferred FK failures consume both live/category sequence values while rolling back business writes. The combined Live/Comskip fifteen-path code is committed as132d69d2 after SDK81 all four gates (1,188 passed/0 failed/786 ignored). Separate physical84/69, temporary UPDATE4 and TEMP live-transaction4 gates pass, including both real deferred-FK COMMIT failures, awaited rollback/Ready and immediate GET serialization. The earlier physical observer failure remains exit1; its test-only scalar-sequence correction preserves all states without nextval. Candidate53 build passes in310.069 seconds, followed by all six package checks in27.284 seconds with55 scoped CIDs independently absent. The clean public14 attempt exits1 in1,214.148802 seconds:13/14 contexts pass, including all twelve contiguous settings cells; Comskip mark-repeat fails its original observer clock bound and subsequent positive CUT/natural cells are not run. Independent diagnosis supports a prepared exact terminal-journal fence without clock slack; the original failure remains literal. An earlier UID-only0700-directory failure and mode-only repair are retained. Root verifies18CIDs/17 captured host generations/project absent, with the UID generation gap explicit. Corrected full14 and own-input natural-CUT acceptance remain pending; no M5, performance or complete milestone closure follows.

[Positive Comskip reference](../scripts/rust-integration/comskip-positive-source1/README.md)
now includes a real two-range commercial detection and an unchanged target
repeat. The whole controller exits 1 because an unrelated periodic scan and
its EPG child enter the repeat's producer journal. Those publications remain
visible. A separate [real commercial cut](../scripts/rust-integration/comskip-cut-source2/index.json)
now qualifies offline: two commercial ranges and three kept segments reduce
544.021 seconds to 483.113 seconds, with complete audio/video decode. Its
controller also exits 1: the helper expected a custom INI path while Source
reports its default path. That mismatch stays literal; the captured directory
permissions support an explanation, but actual worker argv/getter evidence was
not captured. Sequence equivalence is unobserved for that manual cut capture.
A later cut-repeat is qualified, followed by natural 520-second automatic MARK
and CUT captures. MARK's business assertions qualify offline while its
controller's two-task expectation rejects the real causal artwork publication;
its artwork backend remains unobserved. The following CUT controller exits 0
with all three causal publications and backends. The manual CUT decoded-content
authority covers 12,076 video frames and 22,641 audio frames. Full Native positive
acceptance remains pending.

[Language lifecycle reference](rust-milestone-batch52.md) now passes eleven
Original refresh roots across two accounts, six category-settings PATCH calls
and one declared Episode fixture seed. Complete rows and all eleven sequences
are retained in credential-safe evidence. The nine-path Rust language/API join passes all four SDK77 gates (1,181 tests), read-only84/68 physical checks and four separate rolled-back UPDATE controls, with exact default/public-state preservation. Source `432df6ac` builds as image `cbd8d1b3`; all six package families pass, with 55 exact package CIDs absent and a new Main ELF. The complete Native13 cohort exits1 after the old12 and newlanguage11 local checks pass; strict retained-Source comparison fails. Root verifies sixteen CIDs/fifteen captured host generations and the project absent. Independent full projections isolate six random tag-observer receiver IDs, with no business/PK/sequence/event difference. That literal private failure remains unaccepted. A narrow typed receiver-proof correction now passes a complete clean public13 run in 999.062 seconds using forty public helpers, unchanged Source authority and the same image/SDK77. All eleven language roots compare exactly, with sixteen-CID/fifteen-generation cleanup. Nonempty live-group React saves remain a separate Source/Rust gap; performance and full milestones remain open. SDK75/76 Clippy failures and the before-Docker archive-checksum preflight failure remain literal.

[Candidate51](rust-milestone-batch51.md) has seventeen Source observations for
international values, ordered identities and 1,000/2,001-row catalogue lifecycle.
The original controller's scale-fixture failure and the separate scale-only pass
remain explicit; twelve completed roots were reused without replay. Its eight Rust paths now pass all four SDK74 gates with 1,174 tests. Separate
physical checks pass readonly47/30 and four actual temporary-table UPDATE
controls, rolled back with exact public rows/sequences and cleanup. Source
`63e66725` builds as image `f7484742`; all six package families pass with 55 exact
containers absent. The complete Native12 cohort passes in 842.643 seconds, including strict
full-row/all-eleven-sequence comparisons for all seventeen new Source roots,
pagination and snapshot custody. Root verifies fifteen exact containers,
fourteen captured original host generations and the project absent. Clean public12 also passes all twelve contexts in 847.196 seconds from archive `149ae285`, using 38 public helpers and published safe Source authority only, with the same fifteen-CID/fourteen-generation cleanup scope. No known Source business root is replayed; new performance and complete milestone acceptance remain pending.

[Candidate50](rust-milestone-batch50.md) adds the bounded, empty-language
Movie/Series external-ID identity slice. Source passes nine characterized roots;
SDK70 passes all four gates with 1,169 tests, and physical checks pass 45
production statements and 24 wrong-bind controls with cleanup. SDK68's sandbox
TCP-bind failure and SDK69's passing host diagnostics remain preserved. Its Docker image and all six package families now pass with all 55 package
containers absent. The first full Native nine-context attempt exits 1 after all eight prior
contexts and nine new local cases pass: strict Source comparison catches real
relation-PK allocation order drift. The minimal production ordering correction now passes SDK71 and fresh 45/24
physical checks; the corrected image and all six package families now pass. The full affected
Native nine-context rerun passes in 605.043 seconds, including all nine strict
Source comparisons, exact relation PKs and all eleven sequences. Root verifies
twelve exact containers, eleven original host generations and the project absent.
The failed first attempt remains separate; no PK waiver or Source-oracle change
is used.
The clean public nine-context reproduction also passes using only published
credential-safe Source evidence; its Native coordinator exits 0 in 603.457
seconds, with exact cleanup. New performance and complete milestones remain open.

[Candidate49](rust-milestone-batch49.md) removes the temporary shared-account
metadata refusal. Source passes its four-root shared update/prune cohort, SDK67
passes with 1,163 tests, and physical checks pass 42 statements and 18 wrong-bind
controls. Its image, six package families and complete Native eight-context
cohort now pass, including strict shared-account Source comparison and cleanup.
The clean public eight-context reproduction also passes using published safe
Source evidence without private captures. Its Native coordinator exits 0 in
474.460 seconds; the direct launcher has no separate whole-launcher timing or
outer EOF/group instrumentation. New performance and full milestones remain open.

[Candidate48](rust-milestone-batch48.md) implements bounded ordinary Movie/Series
list metadata and logo lifecycle. Its six-case Source cohort passes, all SDK66
gates pass with 1,164 tests, and physical checks pass 44 statements and 22
wrong-bind controls. Its image, all six package families and the corrected full
Native seven-context cohort pass. The original exit-1 comparison on differing
assigned fixture URL origins remains recorded separately; the corrected run
changes no Rust code or known Source authority. The clean public seven-context
reproduction also passes without private Source captures. New application
performance and all full milestones remain open.

[Candidate47](rust-milestone-batch47.md) adds the Source-characterized bounded
advanced-Series refresh and Requests CA trust for manual provider-info sessions,
including exact category/relation sequence allocation. SDK65 passes all four
gates with1,158 tests; physical37-statement/10-control schema checks pass. Its
committed source builds, passes six package families and the combined six-context
Native cohort (including provider-info5 and advanced-Series4). Its fresh12-phase
viewing study is valid but fails one CPU pair (0.9284 >0.90); observed CPU/working
memory estimates fall about10.6%/70.7%. The clean public29 six-context run also
passes; resource acceptance and full milestones remain open.

A separate fresh persistent-observer comparison now passes all six scoped
one/two-viewer budgets across twelve valid phases. Observed CPU/working-memory
ratios are 0.71565/0.29729, with equivalent useful media. All observer costs remain
charged; no Rust application change or retroactive correction of the failed
study is claimed. Full 24-phase, active-work and full milestone acceptance remain
open. The [Candidate47 record](rust-milestone-batch47.md#fresh-persistent-observer-comparison)
keeps both protocols and their limits explicit.

[Candidate46](rust-milestone-batch46.md) reuses compiled queue observation scripts
and passes all four SDK63 gates (1,148 tests) plus three explicit physical Redis
tests. Its source builds with actual changed-crate recompilation and passes all six
package families and the clean public four-context application cohort. Fresh paired
performance validation is deferred to the combined TLS/advanced-Series successor; Candidate45's failed
CPU pair remains part of its published evidence. Full M0–M9 acceptance stays open.

[Candidate44](rust-milestone-batch44.md) joins the recording catchall correction
and Source-characterized basic Series ingestion. SDK61 passes all four gates
with 1,148 tests. Its CLI-successful Docker attempt is untrusted because a
normalized timestamp/cache interaction reused stale executables. No package or
Native run was dispatched. [Candidate45](rust-milestone-batch45.md) corrects
compile freshness, passes all SDK62 gates and six package families, and passes
the four-context Native batch (repeat5, Series4, Comskip3, shared loaded VLC).
The12-phase current live study is valid but misses its CPU target in one of
six pairs (ratio0.9053 >0.90); observed memory reduction is about71%, while
resource acceptance remains open;
Candidate43 startup and fixture failures remain unchanged below.

The [Candidate41 source](rust-milestone-batch41.md) passes all SDK58 gates
with 1,145 tests and the physical 19-statement/six-negative SQL check. It adds
bounded VOD updates and manual commercial detection. Its first Docker build
fails on a Comskip/shared-media library hash collision; a separately isolated
runtime bundle is the next correction. No Candidate41 image or Native result
is qualified. The [Candidate42 successor](rust-milestone-batch42.md) isolates
those libraries and passes all four SDK59 gates. Its build passes, while actual
Comskip invocation fails the package gate on embedded RPATH library selection.
The [Candidate43 successor](rust-milestone-batch43.md) applies the observed loader
fix and reusable Docker compilation layout; SDK60 passes all four gates, with
Docker build and all six package families passed. The combined Native run
fails on a recording-route overlap and rootless fixture-copy ownership; a
successor correction and Source-characterized basic Series ingestion are in
validation. The changed XMLTV Docker target passes 14 tests and 1,144 retained
parser comparisons. Full milestone and current application resource gates stay open.

The retained [Candidate40](rust-milestone-batch40.md) passes SDK56 with
1,141 tests and all six package families. One combined application batch passes
all eleven cases: aggregate refresh four, real Native station HTTP four and
fresh basic Movie ingestion three, retaining Source55/53/58 without replay.
Actual exit is zero in 164.960 seconds; all five fixture containers and recorded
positive original host PIDs are absent. This delivers the aggregate broker-child
correction and qualifies the bounded station HTTP and fresh VOD workflows.
The clean-commit Docker verifier passes both focused public contract groups;
the full verifier CMD was not executed. Full milestones and
new matched-work application performance acceptance remain open.

The preceding packaged [Candidate39](rust-milestone-batch39.md) passes SDK53 with
1,135 tests, the physical 15-statement schema check and all six package families.
Native57 passes all four station worker/ETA cases. Native56 preserves its actual
aggregate child-admission failure after a successful zero-active case and parent
publication; Candidate40's successor correction passes the affected aggregate
cohort without changing that historical failed result.

The preceding packaged runtime is
[Candidate38](rust-channel-matching-native.md): all four SDK51 gates pass with
1,130 tests, the physical 11-statement schema check and all six package families
pass, and Native52 passes the complete 14-case matching cohort with retained
Source51 authority. The no-download/absent-model/empty-plugin scope is explicit;
Main54's seven-context regression passes six contexts, including shared VLC and
unchanged React, and retains input promotion's strict decoder-startup failure.
Full M6, broader application acceptance and new performance acceptance remain open.

The preceding packaged candidate is [Candidate37](rust-milestone-batch37.md): all
four SDK49 gates pass with 1,117 tests, zero failures and 783 ignored. It adds
Connect delivery and enrichment at actual claimed normal-recording start/end,
using short event sessions on the existing Auth pool and template runtime.
General DVR plugins, direct HTTP deletion and recovery hooks remain
explicitly guarded. The exact Docker image and six package families pass. Native48 passes the
named empty-plugin claimed-recording Connect slice, including actual stopped
and completed events, strict Source payloads, caught delivery failures and
history trimming. Five unchanged Native47 workflow passes retain their scopes;
nonempty restart still fails strict media validation. General plugin and
per-event physical settlement acceptance remain open. The [portable recording
workflow](rust-recording-connect-public-tests.md) also passes its fresh Source→Native
default pair without private historical captures; full M0 remains open. Earlier
image results retain their own scopes. All full milestones and matched-work
performance acceptance remain open.

The preceding source candidate is [Candidate36](rust-milestone-batch36.md), runtime
`350b4fdacc45ea7ec4136398d7e7bae4ba4b1f73`: all four SDK47 gates pass with
1,115 tests, zero failures and 783 ignored. It allows unrelated Main startup
with a populated plugin directory while refusing unsupported DVR effects before
queue claim or producer mutation. Docker and all six package families pass;
Main39 passes complete Guide and grouped recording/settings. Operations42
qualifies selected M8 actions, isolated strict DVR queue/API safeguards,
current-schema restore and partial-admission cancellation. The earlier
recording/recovery restart retains its strict audio-DTS failure observed in
both Source and Native. General DVR plugin execution, Source three-root startup
parity and enabled Native restore remain open. The [DVR compatibility analysis](rust-dvr-plugin-compatibility.md)
identifies the remaining recording-event and constructor-extension interfaces.
All full M0–M9 milestones and new
full-service performance acceptance remain open.

The [operations batches](rust-operations-candidate36.md) retain their failed
workflow checks and independently absent owned containers. Operations41 passes
all five Source normal-restore checks. Operations42 passes its selected Source
characterizations and Native M8 actions, isolated strict DVR safeguards,
current-schema restore and partial-admission cancellation, with exact cleanup. The [paired unchanged
React workflow](rust-milestone-batch36.md#paired-unchanged-react-workflow) now passes
all five browser assertions against Source and Rust, including decoded video
and active-channel Stats refresh. [Earlier browser failures](rust-react-candidate36.md)
remain literal. These scoped results establish no full milestone or performance
acceptance.

The preceding [Candidate35](rust-milestone-batch35.md), runtime
`b0fc1ab1085ae765cbc978b349f08171066edfff`, passes all four SDK46 gates and
all six Docker package families. Main37 reached validated real plugin startup
and nonforced physical exit 0. Its whole Guide run exited 1 on a local XML
audit-baseline assertion; other groups, including all seven download cases,
passed. Fresh Source36 passes its complete 36-case guide reference. A scoped
audit-delta observer correction is prepared; fresh Native acceptance is pending.

The preceding source candidate is [Candidate34](rust-milestone-batch34.md), runtime
`8c872439590511258750b31d856432e553f8192f`: all four SDK45 gates pass with
1,111 tests, zero failures and 783 ignored; its explicitly tagged Docker build
and all six package families pass. It adds bounded, opt-in failure diagnostics,
with no HTTP pool or bootstrap behavior fix. The Native startup/guide diagnostic batch completed with two failures; its
actual diagnostics identify a missing namespace and an earlier DVR guard. [SQLite compatibility](rust-sqlite-compatibility.md) remains an
explicit Source-characterization and implementation gap. All full milestones
and new full-service performance acceptance remain open.

The preceding source candidate is [Candidate33](rust-milestone-batch33.md), runtime
`32d5135dc084dd8656599cfedd800fc9473b55c9`: all four SDK43 gates and six
Docker package families pass. Its actual plugin launcher now invokes the existing
bootstrap main and emits valid command-line help. Its affected three Native workflows completed: the held control passes, while
guide and M8 startup failures remain. Prior independent observations retain
their own image and scope. This is not yet enabled-plugin/RPC acceptance. All full M0–M9
criteria and new full-service performance acceptance remain open.

The preceding source candidate is [Candidate32](rust-milestone-batch32.md), runtime
`143c8638540ec3105d774a82f947289c5a72367e`: all four SDK42 gates pass with
1,106 tests, zero failures and 783 ignored. Its immutable Docker build and six
package families pass. It connects the bounded current-schema restore path and
fixes the Main signal/server race by joining the same physical cleanup owner.
Its completed Source2/Native10 batch passes both Source and seven Native
contexts, with guide/M8/held failures preserved; restore and browser callbacks
remain unexecuted. Historical and enabled-plugin restores stay guarded.
All full milestone criteria and new full-service performance acceptance remain open.

The preceding source candidate is [Candidate31](rust-milestone-batch31.md), runtime
`ca2e173c797b050dc42ef897501a8fcd5db0f552`: all four SDK41 gates pass with
1,099 tests, zero failures and783 ignored. Its six package families pass on image
`sha256:6cd3617f698006c85bdb7696f3f3b2e441b101aa885d6be9b96ab6635f7ef8bf`,
with exact Git/SDK inputs, unchanged React, Original credits and owned cleanup.
It corrects the actual Source guide task names rejected by Main30. The corrected
Main31 Source-first Compose batch is pending; package checks do not establish
application or performance acceptance.

[Main30](rust-milestone-batch30.md) remains a failed application run: Source
Redirect and16 multipart file cells pass, while guide fixture/observer, UDP timing
and child-consumer failures remain literal. Native attempted only its first guide
context; the other nine contexts are not run. Owned cleanup passes. The
[Candidate29 diagnostics](rust-milestone-batch29.md) preserve separate failed and
scoped successful observations, including the unresolved plugin shutdown scope.
[Candidate28](rust-milestone-batch28.md) retains its selected M4 and other scoped
qualification; none transfers full acceptance to Candidate31. All full M0–M9
milestones and new full-service performance gains remain open.

The earlier [Candidate28](rust-milestone-batch28.md) SDK passes1,088 tests,
zero failures and783 ignored. It joins local/remote XML event scopes,
dummy/ordered-name guide output, native builtin task handoff and late-body guards,
process transports, inactive input profiles, Channel/Recording Collector and
account scalars. Its observations retain their own candidate and workflow scope.

The preceding source candidate is [Candidate27](rust-milestone-batch27.md): all four
SDK gates pass with1,078 tests, zero failures and782 ignored. It adds the normal
SD Connect/scoped-plugin event caller, supervised unknown-SQL hook quarantine/
readiness and numeric preview command substitution. Docker/package checks pass. Main, enabled-plugin
physical execution and paired performance remain required.

[Candidate26](rust-milestone-batch26.md) passes1,075 SDK tests and all four gates.
It joins inverse Redirect/scoped previews, remote/staged XMLTV, ordinary guide
output and experimental plugin lifecycle ports. The corrected `d96d5d13` image
passes Docker/package checks, including the explicit Original Python compatibility
closure. Actual Main/enabled-plugin execution and performance remain unaccepted.

The preceding source candidate is [Candidate25](rust-milestone-batch25.md): all four
SDK gates pass with1,061 tests, zero failures and775 ignored. It joins common
Redirect assignments and native proxy alias sharing with guarded plugin runtime,
SQL/result/cache infrastructure and owned host deadlines. Docker/package and
all eighteen joined physical controls pass. Selected affected workflows pass
across the original Main run and corrected paired Redirect context; original
failed rows remain. Enabled-plugin and full milestone/resource acceptance remain
required.

The preceding source candidate is [Candidate24](rust-milestone-batch24.md) at
`df815fbb3d312d191654bf4079f337ecae216430`: all four SDK gates pass with
1,053 passing tests, zero failures and 767 ignored. It joins normal guide jobs,
XMLTV busy deferral, recording profiles, generation fencing and the configured
media-root alias. Docker/package checks pass. All eleven physical controls pass across identified attempts after the
fMP4 fixture supplies multiple keyframes. The Source-first Main batch exits1
with exact cleanup: paired SD8, auto5 and recording-profile4 pass; Native
compact3/API6/output6 pass. Observer/scenario and regex-state errors keep
whole Main and performance acceptance open.

The preceding source candidate is [Candidate23](rust-milestone-batch23.md): all four
SDK gates pass with1,034 tests, zero failures and764 ignored. It corrects follower
retirement while profile demand remains. All eight real Redis controls and
Docker/package checks pass; combined Main and performance acceptance remain open.

The preceding source candidate is [Candidate22](rust-milestone-batch22.md): all four
SDK gates pass with1,034 tests, zero failures and764 ignored. It joins shared
output-profile ownership across TS/HLS/fMP4 and corrects post-await stop admission.
Docker/package checks and seven Redis/stop controls pass. The two-replica viewer
control fails; combined application and performance acceptance remain required.

The preceding source candidate is [Candidate21](rust-milestone-batch21.md): all four
SDK gates pass with1,029 tests, zero failures and761 ignored. It resolves the
supported media alias for metadata spools and joins normal compact/manual
numbering and input-profile promotion. Docker/package checks pass; combined
application acceptance and performance remain required.

The preceding source candidate is [Candidate20](rust-milestone-batch20.md): all four
SDK gates pass with 1,025 tests, zero failures and 761 ignored tests. It corrects
the actual Main AutoChannels registry ceiling mismatch and drives owned regex
drain before lane joins. Its Docker build and package checks pass; combined
application acceptance fails with seven passing Native contexts, five failures
and incomplete Source comparisons. Exact19-container cleanup passes. Resource
acceptance remains required.

The preceding source candidate is [Candidate19](rust-milestone-batch19.md): all four
SDK gates pass, with 1,023 tests, zero failures and 758 ignored tests. It joins
normal auto-sync, full metadata, shared channel deletion, dependency-only regex,
remote stream health and Source-equivalent adaptive Beat timing. Its second
Docker build passes, with archive/credit, crypto, PostgreSQL and paired VLC
useful-core checks. Actual final-image regex import exposed Debian site-directory
discovery. The third image passes final package checks, including framed regex,
but actual Main startup fails the registry ceiling check corrected in20.
Combined application/resource acceptance remains required.

The preceding code is [Candidate18](rust-milestone-batch18.md): formatting,
compilation, strict Clippy and1,008 tests pass (zero failures,756 ignored).
Its real HTTP regression fails on the preceding engine and passes after the
local healthy-owner keepalive correction. Its Docker build and physical
asset/credit, GPG and PostgreSQL probes pass. VLC remux acceptance exposed a
plugin discovery defect; the launcher correction awaits a rebuilt image.
Combined application acceptance and remote-owner health remain open gates.

The preceding code `49ad42e2f48ea11f3d6a991e8bbb45702d89906f`,
[Candidate17](rust-milestone-batch17.md), passes formatting, compilation, strict
Clippy and1,004 tests (zero failures,756 ignored). All1,260 SDK inputs match
committed Git blobs. It adds plugin actions/managed ZIP installation, remote
playlist/full-result handling and VLC/grouped-credential input work. Its Docker
build and application acceptance remain pending.

The historical Candidate25–18 build checkpoint below is retained for its original
scope; the current candidates are listed above. Candidate25 was built at
`6612ec14757bdfb7f31a3e546ed9a6cbd8a7ad7d`; package and eighteen joined
physical controls pass; selected Main workflows are qualified across named
executions, while full milestone/resource acceptance remains open. Candidate24 at
`df815fbb3d312d191654bf4079f337ecae216430`; package and eleven physical
controls pass; the Main batch retains qualified successes and failed fixture
assertions. Whole Main and resource acceptance remain open. Candidate23 at
`7e0dd3dd51ae040467d9ea44465843d029e17775`; package and eight physical controls
pass, while full Main acceptance remains open. Candidate22 at
`0752ba808cf657c8c0371d75ffdc65de96ecd9e0` retains its failed replica control and
three passing Native application contexts. Candidate21 at
`ac9d704943e69b9c343f31063cda1e3df5008791` also passes package checks. The preceding Candidate20 at
`1cdf0c15ff4d8265409337f6b0ce29a0debe29e3` starts actual Main.
Auto-sync completion and live VLC acceptance remain open. The preceding built
runtime is Candidate19 at `4f9ba334bf5db99b982a4f7ff91774bb9da45094`.
The preceding built runtime is Candidate18 at
`8cf704d2794ec3230b3c2856fd66dffe2d01720d`.
The preceding runtime
`921c5c40b2eb7a7073a0006b43fae86b41cd2728`:
[Candidate16](rust-milestone-batch16.md) passes984 tests and package probes.
Its [functional observations](rust-batch16-functional-observations.md) include
nine passing Native contexts, fresh paired idle viewing and seven paired XC
business cells. Original strict fixture failures remain recorded. The separate
resource run retains [premeasurement startup failures](rust-candidate16-resource-startup.md)
and a [descriptive paired capture](rust-batch16-resource-observations.md) with
fixture accounting bias and Native media-validation failure. No gain is accepted.
The corrected runtime/harness must complete the full fixed paired workload.
No full M0–M9 milestone or new performance gain is accepted.

The [Candidate15 grouped run](rust-batch15-functional-observations.md) passes10 of19 Native contexts and all ten restart state predicates, with26-container cleanup verified. A separate pinned review qualifies all six retained Source/Native media files without rewriting original strict failures. Fixture failures and a real proxy/NO_PROXY transport gap remain; no full milestone or new performance gain is accepted.

The earlier SDK runtime is `71055d14908c02588b205b564752a25f8cee9760`: [964 workspace tests and the real-Redis durable ETA test pass](rust-milestone-batch15.md). It adds natural plugin startup/periodic scheduling; later grouped observations above retain their own image and scope. Full milestones and new performance remain open.

The preceding SDK runtime is `f2602aa54e0a50eead6e9c9f0bc7f7c0b3658cad`: [960 workspace tests plus three real-Redis ownership checks pass](rust-milestone-batch14.md). Provider runtime acceptance and new performance remain pending; all full milestones remain open.

The preceding SDK candidate is `b646f64f49ff0b4b4cd85af9d9051793c2c584f2`: [952 workspace tests pass](rust-milestone-batch13.md), with new plugin/XC handlers whose runtime gates remain open. The newest measured functional scope remains runtime `8faa6b9c444c8cdfec6cdfd3d1baa4d585ab2b2f` ([945 tests](rust-milestone-batch12.md)): its [grouped Compose run](rust-batch12-functional-observations.md) passes12 of14 Native contexts, all12 normal backup cells and five paired/Native physical backup checks. Core/restart fixture and observer failures remain literal; the event precision runtime assertion was not reached. All19 containers and both project inventories cleaned up. The earlier [Candidate11 observations](rust-batch11-functional-observations.md) retain their own scope. All full milestones remain open; no new performance gain is accepted.

The earlier packaged recording candidate is `cb1ba69af8e0623222391a707a2b9fc03b5995f6`:
[915 tests and its Docker build pass](rust-milestone-batch8.md), and the selected
worker cohort passes223 checks with strict data-only Native mounts. Playback,
normal artwork and their combined route/drain ownership are assembled. The
[shared recording cohort](rust-batch8-recording-observations.md) now retains
failed queue-consumption and observer results; Native authorization and selective
recording controls passed. These results remain separate from the earlier
PostgreSQL acceptance below. All full milestones stay open, and no new paired
performance gain is accepted.

The preceding committed runtime is `52e8ebc7e4ce8b92368ec94797d60c0e44678687`:
[911 tests pass](rust-milestone-batch7.md), with 751 external-fixture tests ignored,
and its [production image](rust-milestone-batch7-build.md) preserves React and all
original credits. The [grouped PostgreSQL run](rust-batch7-tls-acceptance.md) now
passes all 12 retained cases, including IPv6 and both allow branches. SQLite
remains unsupported; broader TLS branches still need qualification.

The [experimental selected-worker cohort](rust-batch7-selected-m6-observations.md)
passes 222 checks with real recovery/version startup, configured/no-rules import
chains and failure continuation. Its future-head retry interval has 20 actual
reads in about one second, within the declared bound. This is a functional
resource check, not a paired CPU gain. The broad evidence bind exposed Original
source files even though Native had no `/app` source bind; a stricter data-only
fixture is prepared for the next candidate. Earlier
[permission, journal and queue-spin failures](rust-batch6-functional-observations.md)
remain unchanged.

Normal recording HTTP and queued-frame adoption are implemented in this runtime;
playback and broader artwork are under joint validation. Both experimental
worker/scheduler flags remain off by default. Full Source task/role/input coverage,
all default scheduling, SQLite, physical faults, soak/rollback and whole-service
performance remain open. These results do not close M0–M9.

The following candidate5 checkpoint retains its earlier application scope.

The earlier packaged runtime is `95da37d9f0bd6dcca5f678d114bdac994de9b8de`:
[896 default tests pass](rust-milestone-batch5.md), with 751 external-fixture
tests ignored. Its [Docker image](rust-milestone-batch5-build.md) preserves the
React assets and original credits; [all 11 clean-install/React checks pass](rust-milestone-batch5-clean-install.md).
The [packaged job cohort](rust-m6-packaged-main-batch5.md) passes 191 assertions:
configured-rule artwork, actual settings PATCH200, a second real import with
empty rules, and a failed SQL task followed by continued work.

The [candidate5 Core99 cohort](rust-batch5-ops-acceptance.md) passes 99 business
HTTP cases, 2,257 effect checks, 643 row/task checks and 93 additional transport
metadata checks. Five actual publications now include the retained Source
headers; downstream execution and complete property equality remain unqualified.
These are scoped integration results, not full milestone completion.

At that checkpoint the scheduler/worker graph and normal recording HTTP
workflows remained unfinished. The newer runtime above adds experimental
assembly and HTTP code; those changes are absent from the candidate5 image.
The [default worker audit](rust-default-worker-graph.md) lists the selected 14
task families and missing Main integrations. The [Source task audit](rust-source-task-coverage.md)
finds 47 static application task declarations; producers and callbacks outside
the selected graph still need an executable coverage crosswalk.

At candidate5, the [datastore comparison](rust-batch5-ops-acceptance.md) found
8 of 11 PostgreSQL cases compatible; the hostname/IPv6/allow fixes were still
pending. Candidate7's twelve-case acceptance above supersedes that current
compatibility status without changing the earlier failures. SQLite remains
unsupported, and explicit rejection is not compatibility.

Both recent [resource preflight protocols](rust-milestone-batch2-resource-preflight.md)
retain failed useful-work checks. No new performance gain is accepted, and old
measurements do not transfer to this image. All M0–M9 gates below remain open;
older results retain their recorded source and workload scope.

The earlier [PATCH405 and plugin attempts](../scripts/rust-milestone-batches/m478/batch3_evidence/README.md)
remain attached to their original image; the subsequent passing setter workflow
does not rewrite those failures.

The [same-image targeted continuation](rust-batch12-targeted-observations.md) now passes corrected Core and the event-precision predicate. Native restart retains one unestablished expired-input condition; Source passes all ten task predicates and control ordering but fails its declared media no-stderr check. All seven containers cleaned up. The full workflow and milestones remain open.

## Earlier scoped evidence

The [current Core settings library validation](rust-core-settings-current-validation.md) passes whole-workspace formatting, compilation, strict checks and 725 default tests (736 ignored). Its historical component qualifies 32 requests and seven finite ownership predicates; Main integration and full M5 remain open.

The [bounded REST reader library](rust-rest-read-ownership.md) passes the current 851-input workspace gates (741 tests passed, 737 ignored). Two historical physical read-cancellation cases independently pass; adoption into Core/Main and duplicate-key error compatibility remain open.

The [remaining acceptance work](rust-remaining-acceptance.md) separates the measured packaged checkpoint, newer library checks and the executable gates still needed for full parity.

The [inert plugin metadata service](rust-plugin-repositories-current-validation.md) passes current 854-input workspace checks (757 tests, 738 ignored); its confirmed HTML/duplicate-key differences and Main integration remain open.

The [Core retained-reader and error-body composition](rust-core-rest-owner-validation.md) passes 768 tests (738 ignored) on the plugin-inclusive 854-input library. Fresh HTTP/physical acceptance and Main assembly remain open.

[Image58](rust-image58-build-validation.md) builds the newer 854-input library composition from exact committed source and preserves all 114 installed React/credit hashes. This is historical build-only evidence; the historical selected functional and measured image57 acceptance retains its original scope.

The [Source-derived HTML error correction](rust-source500-correction.md) passes the combined 854-input workspace (769 tests, 739 ignored). Its fresh two-request Native acceptance now passes exact HTML status/body/content-type and unchanged SQL/authentication-history checks; earlier historical results are unchanged.

The [VOD HEAD authentication correction](rust-vod-head-authentication.md) passes the current library gates (774 tests, 739 ignored) and peer review. Fresh Native wire/Main comparison and SPA/encoded URL boundaries remain open.

The [plugin metadata evidence packet](../scripts/rust-pluginrepo-contracts/metadata-component/README.md) now reproduces four distinct capture epochs, preserving the original exception/JSON difference and the fresh two-case HTML correction. Root verifies 600 payloads, 15 controls, both Git reconstructions and 631,568 exact private projection nodes. Full M8 remains open.

The [fresh Core bounded-reader replay](rust-core-bounded-rest-component.md) qualifies 32 exact response comparisons and six ownership/capacity cases on immutable `46b91d70`. Root verifies 100 published files, 55 corruption controls, exact private projections and 859-input reconstruction. New DB1 observations are explicit; earlier physical DB0 history observations are limited to DB0. Main/full M5 remains open.

## Progress ledger

The [acceleration plan](rust-migration-acceleration.md) describes how owners
develop and validate the remaining work in parallel without changing these gates.

Statuses: **planned**, **in progress**, **blocked**, **passed**. A blocked runtime
check remains unverified; it is never converted into a pass by documentation.
Update this ledger with report paths and tested commit IDs as evidence arrives.

| ID | Deliverable | Depends on | Current status | Evidence required to pass |
| --- | --- | --- | --- | --- |
| M0 | Reproducible baseline, toolchain, fixtures, and measurement | None | In progress. [Method declaration map](../scripts/rust-contracts/method-declarations/README.md), paired AppData and backup GETs, and [EPG and profile reads](rust-m0-f960875-react-read-wave.md) verified. [Connect delivery log JSON pagination](rust-m0-e7dda7f-connect-log-json.md) matched. Wider executable coverage remains open. | Rust checks; Compose readiness; Python baseline contracts and performance reports |
| M1 | Shared bounded streaming core | M0 | In progress. All [33 core and runtime tests](rust-m1-e68fb95-bounded-stream-core.md) passed on e68fb95. Full M0 acceptance remains a prerequisite. | Buffer, TS, discontinuity, wakeup, shutdown tests; comparative core benchmark |
| M2 | Authenticated live TS vertical slice | M1 | In progress. The [e7dda7f ten-viewer pair](rust-m2-e7dda7f-live-xmltv-performance.md) shared one upstream and measured 53.5% lower backend and infrastructure CPU and 77.6% lower peak working set. | Existing-client playback against both backends; lifecycle, limits, and resource comparison |
| M3 | Complete live playback formats and profiles | M2 | In progress. Both backends passed [planned fMP4 handoff](rust-m3-f960875-planned-fmp4-handoff.md). The [abrupt loss comparison](rust-m3-f960875-original-abrupt-fmp4-comparison.md) observed follower EOF in both arms and different remux lease retirement. | HLS/fMP4/profile parity, decoder checks, failover and fanout performance |
| M4 | VOD and timeshift parity | M2 | In progress. Both backends passed [growing DVR playback](rust-m4-f960875-growing-dvr-playback.md) with two playlist generations. Wider recorder, seek and lifecycle checks remain open. The [current default media workflow](rust-m4-m5-e7dda7f-parallel-release.md) passed all four Original and Rust VOD and timeshift phases. | Range/seek/catchup and session parity; throughput, CPU, and memory comparison |
| M5 | Accounts, settings, and channel control APIs | M2 | In progress. [ARM64 React account workflows](rust-m5-f960875-arm64-account-browser-pair.md), [ARM64 React profile mutation and restore](rust-m5-m8-e68fb95-arm64-profile-schedule.md), and [canonical Channel Batch name and wire checks](rust-m5-88b9f45-channel-batch-name-wire.md) passed. The [current configured Proxy Auth workflow](rust-m4-m5-e7dda7f-parallel-release.md) passed 20 policy comparisons. [Two Connect HTML Accept differences](rust-m5-e7dda7f-connect-log-browsable.md) were reproduced. | API/permission/database/React workflow parity and API benchmarks |
| M6 | Provider ingestion, guide, matching, and scheduled jobs | M5 | In progress. [Six-default scheduler installation](rust-m6-f960875-cold-warm-scheduler-install.md), named Version and [three named Cleanup checkpoints](rust-m6-e68fb95-named-cleanup-checkpoints.md) passed within their measured scopes. [Ordinary Rust AIO recurring-repeat](rust-m6-88b9f45-aio-recurring-repeat.md) matched the Original booking and job outcomes. The [current AIO linked XMLTV schedule](rust-m6-e7dda7f-aio-linked-xmltv-cron.md) matched the retained Original scheduled ingest. | Import/job parity; failure recovery; import and concurrent-playback benchmarks |
| M7 | Output protocols and realtime/integration behavior | M3, M4, M5, M6 | In progress. [Direct M3U](rust-m7-f960875-direct-provider-m3u.md) and [assigned numbers across M3U, HDHomeRun and XMLTV](rust-m7-f960875-assigned-numbers-guide.md) matched their paired scopes. [Current Xtream and ARM64 output checks](rust-m7-88b9f45-xtream-arm64-outputs.md) passed. | Xtream/HDHR/XMLTV/M3U/WebSocket/integration suites and performance reports |
| M8 | Backup, admin, plugins, and operational compatibility | M5, M6, M7 | In progress. Backup inventory, download and [ARM64 same-schema restore](rust-m8-f960875-arm64-same-schema-restore-pair.md) passed on both backends. [ARM64 backup schedule GET](rust-m5-m8-e68fb95-arm64-profile-schedule.md) matched for default and saved settings under the documented runtime roles. [Matched modular backup and plugin startup](rust-m8-88b9f45-matched-modular-startup.md) verified the same schedule rows. The [ARM64 disabled backup schedule roundtrip](rust-m8-88b9f45-arm64-disabled-backup-scalars.md) matched both backends. Wider admin, plugin and restore coverage remains open. | Restore/rollback/plugin/admin/config tests; full-service resource accounting |
| M9 | Full parity, soak, packaging, and cutover rehearsal | M0–M8 | In progress. f960875 passed 16 grouped rows and a separate [two-hour soak](rust-m9-f960875-two-hour-soak.md). Lappy passed [three concurrent isolated scopes](rust-m9-f960875-arm64-three-fixtures-pass.md). The [88b9f45 concurrent rerun](rust-m9-88b9f45-three-fixture-retry.md) passed all three scoped rows after the Chromium scratch path fix. The [e7dda7f rerun](rust-m9-e7dda7f-three-fixture.md) passed the same three rows on the latest packaged application. Full release acceptance remains open. | All contracts covered; Compose end-to-end, soak and performance acceptance; rollback rehearsal |
| M10 | Controlled rollout | M9 | Proposed. [Ordinary native ARM64 e7dda7f package and startup](rust-m10-e7dda7f-native-arm64.md) verified. Controlled rollout remains open. | Scoped rollout plan, monitoring and rollback on the selected release image |
| M11 | Default switch and operator handoff | M10 | Proposed. The [local warm rollback](rust-m10-m11-f960875-local-rollback.md) passed on f960875. Default switch and operator handoff remain open. | Released image and configuration, upgrade evidence, attribution and monitored fallback |

The historical selected functional application checkpoint is [image57](rust-image57-validation.md), immutable `d77f25fa94ee128c79ade9176d8eb4a9d23bf498`:839 Rust inputs,495 unchanged React inputs and original credits. All28 package groups and three scoped Main gates pass. Its [portable checkpoint](../scripts/rust-integration/image57-checkpoint/README.md) passes Root's57 controls,487 exact private projections and app/support/SDK reconstruction. The additional26 Main catalogue responses qualify separately while their original controller exit1 remains literal; its failed backend observer is not relabeled by the separate successful stop-observer run. Full M0–M9 acceptance and default worker activation remain open.

[Image57 live performance](rust-image57-live-performance.md) is a historical measured checkpoint: nine fixed alternating pairs, median CPU reductions at1/10/100 viewers of69.33/78.85/91.17%, and peak working-memory reductions52.08/46.30/49.63%. All666 readers,1,080 raw samples/1,062 in-window samples,1,998 media/decoder hashes and independent exact cleanup pass. Root also checks76 controller controls and85 public-suite methods. Worst paired throughput is1.55% below Source. The measurement covers the live HTTP backend plus common PostgreSQL/Redis/local origin; it does not cover the full application or later libraries. Image55 gains remain a separate historical experiment.

The [inert clocked-recording envelope parser](../scripts/rust-job-contracts/clocked-recording-envelope/README.md) adds two source inputs to the callback union:844 Rust/five coupled. Fresh whole-workspace formatting, compilation and strict all-target linting pass, with716 tests passed/734 ignored. Seven Source-wire tests and the unchanged legacy refusal regression also pass in the package qualification. The original application's [real future model→Beat→DVR recording reference](../scripts/rust-job-contracts/clocked-recording-source/README.md) now exits0 with17 checks, stable MKV and full decode; native media execution and its physical gates remain next. Plugin repository metadata has a new actual24-case Source reference with independent cleanup; Native/plugin execution/fullM8 remain open.

The [842-input bounded callback library](rust-display-timezone-callback.md) passes fresh workspace checks with709 tests passed and734 ignored. Its separate historical component passes19 requests and six physical cases with independent cleanup. Main activation, whole-endpoint/timezone-catalogue behavior, cache serialization equality and performance remain open; the physical execution does not transfer to the current union. Its [historical execution packet](../scripts/rust-contracts/display-timezone-callback/README.md) now reproduces all845 source hashes and257 shipped inputs, with26 portable controls, strict140-field credential projection and747 byte-exact lifecycle members independently reviewed.

The [839-input backup evidence union](rust-union839-backup-validation.md) passes fresh formatting, compilation, strict all-target linting and 698 default test executions (732 fixture-dependent tests ignored). Two historical physical packets preserve all failed attempts: four callback predicates qualify; real create/status Rust cases pass with a separately accepted source-derived idle-counter correction, while their original host failures remain literal. Root checks 1,036 byte-exact lifecycle members, 51 portable controls, historical reconstruction, twelve credential projections and fresh cleanup. No current-source physical, Main, full M8 or new performance acceptance is inherited.

The [837-input local artwork union](rust-recording-local-artwork.md) passes fresh workspace formatting, compilation, strict all-target linting and 698 default test executions (729 fixture-dependent tests ignored). The separately executed834-input artwork component passes24 checks with two actual Native children: first update/event, duplicate no-write/no-event, both Original results `ok`. Root verifies all 36 public evidence files, exact839-input reconstruction, 125 diagnostic clock observations and fresh cleanup. Its [225-path follow-up](../scripts/rust-job-contracts/recording-artwork/followup/README.md) now qualifies seven physical modes/83 Native checks and four refusal domains, preserving one pre-Native setup negative. Root independently restores87 captured reports, checks11 passing acceptors and the negative, and verifies public/controller controls and source reconstruction. Default task graph/media/Main/fullM6/performance remain open. Image56 is built/probed from the earlier830-input source; its SDK/package/Main validation is separate.

The [830-input XC/Main list source](rust-xc-main-lists.md) passes fresh formatting, compilation, strict all-target workspace linting and690 default test executions (728 fixture-dependent tests ignored). Main opts into the two catalogue list actions, but a later static review found their writer remains test-only and the release branch rejects them. The capability hook alone does not implement release list execution; a successor source/image and actual Main requests are required. Info stays guarded and default workers stay off. The separately executed825-input cached-info gate passes12 retained HTTP comparisons and15 physical controls at its fixed eligibility clock. New830-input packaged Main and performance acceptance remain pending; image55 remains the selected functional/measured application.

[Image57](rust-image57-validation.md) builds the exact839-input [four-path XC release repair](rust-xc-release-lists.md), preserves React/credits and passes its SDK,28-package and three Main groups. Fresh release-source workspace checks pass698 tests/732 ignored. Normal Main catalogue26 semantics qualify, with the separate original observer failure explicitly retained. The later [five-producer Ready/pool-return repair](rust-xc-late-ready-component.md) now has ten qualified finite historical physical predicates and fresh845-Rust/five-coupled workspace checks (716 passed/735 ignored). Its three original controller failures and observed1,023-byte PG activity-prefix correction remain explicit; historical execution is not transferred to the current union or image57. Info actions and default workers remain guarded; this is no fullM7 or performance acceptance.

The [cached XC info packet](rust-xc-cached-info-component.md) now publishes twelve literal retained body/status/content-type comparisons and fifteen actual physical controls on historical825-Rust/five-coupled source. Root independently checks all47 new paths,51 public corruption controls, exact historical reconstruction through the pinned public catalogue sibling, SQL/stdout bindings and finite credential projections. The fixed eligibility clock rewrites no SQL timestamps and proves no current-time freshness, synchronous provider refresh, production info/Main or performance. Seven historical payloads stay separate from the newer canonical gateway and are never used to overwrite it.

The [CoreSettings Source32 reference](rust-core-settings-source32.md) now qualifies32 fixed Original persisted-API-key/APIClient requests, with real resolver/authentication and declared SQL/cache effects. Root independently recomputed the actual private effect predicate, verified69 snapshot projections/2,177 Redis evidence rows,13 public corruption controls, exact138-input Git reconstruction and fresh owned cleanup. Raw capture-only `passed:false` stays preserved; public checks cover explicitly privacy-adapted semantics. TCP/Native generic settings REST, task-coupled writes, browser delivery, Main and performance remain open; no milestone or fully verified endpoint is added.

The [notification form Source12 authority](rust-notification-forms-source12.md) now qualifies twelve captured Original framework observations through a separate offline input-size correction. The executed controller exit1 remains preserved; the original presealed upload is21 bytes, while its predicate expected20. Root independently verifies all61 offline and32 portable controls, exact160-input reconstruction and the credential-safe projected evidence. The later Native form replay qualifies bounded framework/SQL behavior, while complete callback/cache effects remain open; this adds no fully verified endpoint, milestone closure or performance gain.

The [829-input recording/form union](rust-union829-validation.md) passes fresh whole-workspace formatting, compilation, strict all-target linting and 687 default test executions (723 fixture-dependent tests ignored). Its separately executed parented-recording and notification-form components retain their exact historical source bindings and negative attempts. Native artwork/default recording graphs and notification global callback/cache parity remain open; no combined Main, milestone or performance acceptance is added.

The prior selected functional application checkpoint was [image54](rust-image54-validation.md), built from `044f3ad0fb3ee68ffb17b23bbb796ccdae954298` with 792 Rust inputs and the unchanged Original React frontend. All 28 selected corrected package groups and three scoped Main gates pass. The portable packet preserves all 31 package attempts, including the earlier two tooling failures, and the separate pre-claim sandbox refusal. Root independently verified the retained package/Main evidence, 38 corruption/restoration controls, 168 raw-to-public projections and Git-only application/support reconstruction. Full M4/M5, default worker activation and whole-backend milestones remain open.

The [profile and cached XC Source authority](rust-profile-xc-source-authority.md) now publishes 100 literal observations across three distinct Original fixtures and seven successful Native component tests from an overall-failed fixture. Root verified all 37 selected files, 18 controls, 30 private raw bindings, exact privacy projections and three Git-only source reconstructions. Earlier capture and terminal origin-audit failures remain explicit. These observations qualify neither the later XC reader revisions nor Main, full M3/M4/M7 or performance.

The [explicit backup create/status adapter](rust-backup-native16.md) now passes its compiled 797-Rust-input component and actual Native16 HTTP/worker/archive run, plus five task-family refusal controls. Root recomputed the observations and verified all processes, default database bytes and owned cleanup. The committed eight-path adapter remains explicit opt-in; Main/default workers, physical cancellation, packaged dump tools, restart/restore/scheduling and full M8 remain open. The 794-input whole-workspace and image55 gates below retain their earlier source binding; they are not results for this later adapter.

The [806-input configured recording union](rust-configured-recordings.md) now passes strict workspace checks, 611 default tests (712 fixture-dependent tests ignored), 18 focused controls and a fresh nine-case/eleven-check integration run on the combined backup/recording source. Seven remaining fault cases pass 101 checks; the artwork logical-timeout failure remains explicit. These are inert components: parented/default task graphs, recording media, full M6 and its resource comparison remain open. The selected image55 application below remains the earlier 794-input build.

The [824-input cached catalogue component](rust-xc-cached-catalogue-component.md) passes strict full-workspace checks and676 default test executions (721 fixture tests ignored), plus26 retained-Source HTTP body/status/content-type comparisons and11 physical ownership controls. Root independently verifies all54 public controls,501 private projection bindings, the25-path current-source composition, immutable829-input reconstruction and fresh owned cleanup. Existing serde feature graphs and Cargo files remain unchanged. Lists are test-peer staged and four production info actions remain501; new Main/image/fullM7/performance acceptance is not added. Prior readiness, seed and false feature-premise negatives remain explicit.

The [actual parented recording Source reference](rust-parented-recordings-source.md) passes six tasks/five workers and24 checks: real Refresh-to-Series lineage, two local-VODLogo artwork children and deduplication. All22 public evidence files reproduce actual retained bytes; default62/59 manifests and independent owned cleanup pass. Three default descendants remain queued and the future recording task is unconsumed. Native default graph, broader physical fault gates, packaged recording UID/domain coverage and M6 performance remain pending; the later bounded native lineage/artwork components are qualified only in their separate scopes.

The later [794-input source union](rust-union794-validation.md) passes strict whole-workspace checks and591 default tests, with710 fixture tests ignored in that run. Its two separately measured components and three new Source assignments are qualified only in their documented scopes. Its [committed-source Compose SDK check](../scripts/rust-integration/sdkaca6/README.md) and [image55 build](../scripts/rust-integration/image55-build/README.md) now pass. The image55 package/Main replay now passes in its documented scope; new resource acceptance remains separate, and no complete milestone or default-worker activation is added.

[Image53 live measurements](rust-image53-live-performance.md) pass nine fixed paired experiments at 1, 10 and 100 viewers, with 666 authenticated readers and 1,062 in-window resource samples (1,080 raw samples retained). Median paired CPU reductions are 70.11%, 79.10% and 91.29%; peak working-memory reductions are 51.72%, 46.38% and 49.32%. All delivery/resource budgets, 1,998 media/transcript hashes, 30 corruption controls and independent exact project cleanup pass. These results bind only image53/f0/791 and the live HTTP backend plus common PostgreSQL/Redis/local origin. Image54 passes its scoped package/Main gates on the later 792-input notification runtime and retains the same frontend and credits; its performance acceptance remains open. No image53 gain transfers to image54 or the whole backend.

[Image52 live measurements](rust-image52-live-performance.md) historically passed nine fixed paired experiments at 1, 10 and 100 viewers, with 666 measured readers and 1,062 resource samples. Median paired CPU reductions are 69.07%, 78.93% and 91.16%; peak working-memory reductions are 51.63%, 46.56% and 49.24%. These cover the active live HTTP backend plus common PostgreSQL/Redis and local origin, excluding workers and the full application graph. Raw/public numerical controls, all media hashes, input provenance and owned cleanup pass independent review. The [predeclared workload and budgets](rust-image52-performance-plan.md) remain unchanged. Other milestone workloads and full-service acceptance still need evidence; image48 results remain historical.

M1's [bounded core implementation and performance gates now pass](../scripts/rust-core-perf/core8/verified-core8-rebind-1.json). Nine fixed alternating pairs on the exact Core8 source/optimized binary pass equivalent-byte, CPU, memory, pacing and zero-OOM budgets. Median paired CPU reductions at1/10/100 readers are17.1380/70.4178/90.7554%; peak working-memory reductions are29.6326/31.7282/48.4703%. These measure the streaming core plus common PostgreSQL/Redis services. They do not establish image47 or full-application gains. Public exact-source reconstruction, an optimized rebuild, actual1/10/100 functional replay,64 validation controls and standalone plots pass. M1 remains formally in progress because its M0 prerequisite is open; no new core benchmark or unlimited fault-testing requirement is added to its passed technical gate.

M0 now has [guarded Original framework metadata and actual command help](../scripts/rust-contracts/PASSIVE-SOURCE.md), plus [executable Source-only links](../scripts/rust-contracts/SOURCE-LINKS.md) for35 plugin TCP cases across seven routes,166 passive metadata observations and three management-help cases. Nineteen probe controls and17 linking controls pass. The592 route/798 operation/250 non-HTTP identities and unresolved method declarations are unchanged. Passive metadata, Source-only cases and help are separate evidence layers; none marks Native parity or a complete role matrix. The baseline characterization and tooling subgates are now qualified; remaining formal M0 work is executable Source contract assignment. See the [current disposition](rust-m0-m1-disposition-review.md).

[M0 baseline reconciliation](rust-m0-baseline-progress.md) now accounts for the completed warmed live and functional churn evidence. Corrected idle/churn, many/mixed/finite/fault/stream-change/sustained accounting, bounded concurrent imports and the portable literal crosswalk are published. The fixed three worker-inclusive resource repetitions are now published as valid bounded negative references. The [fixed prolonged-outage capture](../scripts/rust-baseline/prolonged-outage/README.md) is now a valid retained observation, with its final fixture-report serialization exit1 explicitly preserved. The actual supplemental Source run now records the remaining measurement categories. [Current workspace formatting and verification-image input closure are repaired](rust-current-workspace-verification.md), with host and actual Compose checks passing for the qualified source. Formal M0 acceptance remains open for executable Source contract assignment; historical bootstrap evidence remains scoped to its original source. See the [M0 closure map](rust-m0-closure-map.md). It does not require completing Native M1–M8 first. The generated register remains byte-identical; its prose count is corrected to11 partial bindings/403 observations.

M0's bounded baseline characterization and current developer verification subgates are now qualified in their exact scopes. Valid Original growth-budget failures and the outage reporting exit1 remain explicit. The af18/4fdf audit found unassigned top-level fields in all798 operations. The first cohort added [13 partial executable Source assignments](../scripts/rust-contracts/EXECUTABLE-SOURCE.md), then leaving785 rows without setup/assertion/result assignments and322 unresolved method placeholders. Eleven operations retain partial captured HTTP cases; two use exact Original framework tests. No full endpoint or Native verification is added. Captured case bindings and the full Original suite do not by themselves assign an executable assertion to every required behavior. M0 and dependent M1 therefore remain formally in progress; the already-passed M1 technical core gate needs no new benchmark. Next work is Source contract/fixture/assertion assignment using real existing tests and captures, not all798 Native implementations or M9 soak. See the [criterion-by-criterion disposition](rust-m0-m1-disposition-review.md).

The [scanner-owned Refresh child component](rust-scanner-refresh-children.md) is now integrated in committed `4fdfc1c4154f8f72ff5ccff5928ef02fe1617cfb`: Root passes 150 actual checks across two functional and three physical-reply cases, plus strict host checks and a [fresh full SDK Compose verification](rust-current-workspace-verification.md). Original creates the scanner children, Rust executes the selected Refresh tasks, and Original consumes every Index/Series descendant. M3U groups/single migration, complete scheduling/recovery, full M6 and new performance evidence remain open; Main/default consumers remain disabled. The current selected application checkpoint is [image52](rust-image52-validation.md).

The account/settings cohort adds16 partial HTTP Source-test assignments from45 named Original workflows and one CoreSettings model candidate from35 helper/cache/serializer tests, all80 individually successful in the qualified3362 suite. No suite rerun, fresh Source/TCP or Native result is claimed. The register now has29 HTTP setup/assertion/result assignments and1 typed nonHTTP assignment. The earlier13 assignments remain unchanged; allother249 nonHTTP entries retain their exact old identity/source/kind/state. All underlying592/798/250 identities,322 unresolved methods,403 existing Native case observations and fully_verified=0 remain. Missing user level in the self-service fixture is actualSTREAMER0. Settings model tests do not establish settings REST behavior; mismatched notification factory paths and a model-only locked-profile test were excluded from HTTP assignments. M0/M1 remain inprogress for remaining executable Source contract work. See the [account/settings definitions](../scripts/rust-contracts/accounts-settings/README.md) and [qualified scope](../scripts/rust-contracts/verified-accounts-settings-source-1.json). Current required-role and input coverage remains incomplete; no formal milestone is promoted.

The [one instrumented Source causal cohort](rust-vod-source-causal-observation.md) now identifies the direct profile write to0 as follower generator11's credential-release EXEC. Other generator4 yielded56 times after that write before its client shutdown and once afterward before generator finally. All four observation layers and owned cleanup pass; Root independently recomputes the six write attributions, validates14 public controls and restores the exact86 executed inputs. This is conditional Source-only evidence, with no new Native request or performance result. Callback indirect effects, ordinary-capacity/next-request compatibility and wider interleavings remain open; Native other-grant preservation and full M4 status are unchanged.

The task cohort adds six exact Celery task candidates to the existing CoreSettings model assignment: seven of250 nonHTTP candidates now have partial executable Original definitions;243 remain static. It reuses74 real worker observations (57 received signals plus17 scanner stage/backend observations without received signals). The file-branches reference has31 before-publish attempts, including two denied LPUSH attempts; its29 queued child frames were not consumed and add no child execution evidence. All29 HTTP assignments and every stable ID/method/source/kind remain unchanged. No new Source run, Native coverage or M0/M1 completion is claimed. The selected host/SDK suite discovers122 tests:121 pass and one existing optional raw-data test is skipped. [Task definitions and limits](../scripts/rust-contracts/scanner-tasks/README.md) retain failures, busy results and observed ACK states without generalizing them.

The [image47 authenticated continuous-live preflight](../scripts/rust-live-perf/verified-live-functional-image47-1.json) passes six Source/native phases at1/10/100 viewers:222 continuous Bearer-authenticated connections, actual Redis user identity assertions,222 hashed2MiB captures and bounded audio/video decodes, and owned cleanup. Four loopback controls cover authentication, no retry and physical close barriers. Successful Source startup-probe diagnostics remain explicit; this is not clean-every-byte or timed-body equality acceptance. The runner collects no resource samples. The later sustained resource checkpoint below is separate; full lifecycle/slot/event/fault/churn and M2 closure remain open; the [portable commands and limits](../scripts/rust-live-perf/README.md) preserve failed setup/capture attempts and older cold100 failures.

The [image47 sustained live-stream resource checkpoint](../scripts/rust-live-perf/image47/README.md) now passes all nine fixed alternating Source/native pairs at1/10/100 authenticated viewers:18 phases,1,062 in-window resource samples and666 continuous readers with bounded media decodes. Median paired CPU reductions are69.89/79.24/91.24%; peak working-memory reductions are51.71/46.56/49.16%. Every pair passes the unchanged throughput, latency, zero-error/OOM and cleanup budgets. Published raw numerical observations, standalone plots,30 complete-run corruption controls and42 controller controls support independent review. This measures the exact image47 active live backend plus common PG/Redis/origin; it excludes background workers and the full application graph. Full lifecycle/slot/event/fault/churn, React playback/statistics and M0 dependency closure remain open for M2.

The [fresh image47 two-worker replay](../scripts/rust-integration/verified-shared-ts-image47-1.json) executes34 checks over six Source/native shared-demand, global-stop and owner-death cases, with12 audio/video decodes. Native lifecycle/recovery passes; healthy Original protocol passes. The combined run remains failed because Original owner death retains one provider slot/input index after90 seconds. Native cleanup takes26.48 seconds without automatic takeover. Both fixtures are removed. This records a recovery improvement rather than identical crash-state behavior; full M2 and performance scope are unchanged.

The [image47 retained-history replay](../scripts/rust-integration/verified-retention-image47-1.json) passes ten live channels through75 seconds,100 late historical readers at32-way concurrency, exact35–55-second-old prefix matches, no extra upstream and20 audio/video decodes. All samples respect4MiB/channel local hot-cache and separate16MiB/channel authoritative-history limits. Parent independently checks numerical sums, capture hashes, decoder frames and owned cleanup. These are functional retained-payload diagnostics, not isolated resource gains. The old16MiB local-expectation failure is preserved. Actual unchanged React playback advances video, but the current Stats workflow exposes missing combined-stats404 and UUID-action405; those concrete M2/M5 gaps are being implemented before new package acceptance.

The [JSON POST channel UUID action](../scripts/rust-channel-uuids/verified-channel-uuids-1.json) now passes39 actual Original/native HTTP comparisons covering roles, profile/adult/level visibility, hidden channels, UUID forms/errors, duplicate/order behavior and stream projection. Seven table fingerprints remain unchanged. The exact705-input workspace passes383 defaults and strict all-target Clippy, with618 external-fixture tests explicitly skipped there; the new fixture test executes separately. The captured Source queryset ordinal is retained through serializer joins, with no sorting of expected responses. Full renderer/method grammar and a new packaged React replay remain open. [The preserved image47 browser failure](../scripts/rust-live-browser/README.md) records the concrete combined-stats404 and UUID405; this source component is not retroactively called an image47 pass.

The [image47 same-runtime live churn checkpoint](../scripts/rust-live-perf/churn/README.md) passes12 rounds/66 authenticated readers and66 bounded audio/video decodes. Each backend stays in the same container for1/10/1/10/1/10 viewers, with one provider slot and no between-round primer/restart/Redis reset. Every round returns owner, client, metadata, provider and upstream counts to zero. Separate Original-only qualification uses identical critical tools. Parent audits all media/transcript hashes and both projects' cleanup; ten public corruption controls pass. This closes that bounded functional churn gate, not long-term resource growth, all lifecycle/fault combinations or full M2/M0.

The [regular default-job library component](../scripts/rust-job-contracts/verified-inactive-notifications-8.json) adds explicit inert constructors for cleanup-null, disabled-recurring-zero and bounded inactive-account notification cleanup. Actual Source/native workers pass134 checks across12 cases, a same-binary58-check regression and nine physical SQL/Redis/Channels controls. The exact704-input whole workspace passes383 default tests and strict all-target Clippy;617 fixture tests are skipped there and separately scoped where executed. Seven immutable Git reconstruction controls pass. Main/default consumers remain OFF. Source SQL-trigger failures currently remain operational/PENDING natively, and active-account evaluation, concurrency, expiry/revocation and complete default schedules remain open; these component results do not close M6 or establish resource gains. [Reproduction and bounds](../scripts/rust-job-contracts/inactive-notifications/README.md) require no private overlay directory.

The [known cleanup expiry/ACK library](../scripts/rust-job-contracts/verified-cleanup-expiry-ack-5.json) preserves actual Source result-before-ACK for expired requests, typed REVOKED, old SUCCESS bytes/TTL, same-worker revocation and body/result completion while an unexpired ACK reply is held. One exact711-input executable passes63 ACK checks,83 expiry/134 inactive/58 pair regressions and15 distinct physical controls. [Public immutable reconstruction and a fresh executable](../scripts/rust-job-contracts/cleanup-expiry/verified-portable-helper-1.json) repeat63+15 without private helpers; seven corruption controls pass. The [parent712-input union](../scripts/rust-job-contracts/verified-cleanup-expiry-parent-1.json), including the committed UUID action, independently passes strict whole-workspace Clippy and390 defaults/624 explicitly skipped fixture tests. All five actual projects are independently absent. Source711 component execution and parent712 default checks retain their different bindings. Main/default scheduling remain OFF; active-account evaluation, external revoke/state-file, generic expiry/IO/visibility and full M6/performance acceptance remain open. [Reproduction and limits](../scripts/rust-job-contracts/cleanup-expiry/README.md) preserve failed/partial predecessors.

The [normal provider-info refresh component](../scripts/rust-vod-api-contracts/verified-vod-provider-refresh-union70.json) handles missing/stale/forced Movie and Series metadata with one shared admission ledger, Source snapshot/task ordering and physical SQL/HTTP retention. Its exact734-input union passes strict workspace Clippy and452 defaults/631 explicitly skipped fixture tests. The same executable passes205 actual Source/native HTTP responses:128 raw-body equal and77 narrowly SQL-bound UUID/clock templates; two Factory clock references remain separate exclusions. All SQL effects/provider-session wire match, alongside five actual queued-task/caller-loss schedules and four physical SQL/Redis gates. [Parent review](../scripts/rust-vod-api-contracts/verified-vod-provider-refresh-parent-1.json) verifies source/ELF/log bindings, all eight fixture cleanups, Git-only734/23-tool reconstruction and three public-path replays. Four omitted datasets were repaired before publication; the failed730-input closure and earlier negatives remain recorded. Main packaging, stale React modal workflows and resource acceptance still require new execution; no prior performance gain transfers. [Reproduction and scope](../scripts/rust-vod-api-contracts/PROVIDER-REFRESH.md) distinguish exact bytes from the77 declared substitutions.

The [bounded active-account notification component](../scripts/rust-job-contracts/active-notifications/verified-active-notifications-10.json) adds an explicit inert constructor for zero/one eligible profile. Source219/17 and native314/17 comparisons cover normal effects, boundaries and explicit operational/unsupported outcomes; seven physical controls and134-check inactive regression pass. Three SQL-fault prefixes remain native PENDING/operational instead of Source Python FAILURE metadata, and title overflow remains pre-ACK unsupported; these are not full result parity. The [new736-input VOD/active union](../scripts/rust-job-contracts/active-notifications/verified-active-vod-union71.json) passes strict workspace Clippy,452 defaults/638 skipped fixture tests, and new-executable314+7/134+9/63+15 actual active/inactive/expiry gates. [Parent review](../scripts/rust-job-contracts/active-notifications/verified-parent-union71.json) verifies source/ELF/log bindings, all owned cleanup, public713 reconstruction/portable replay and a real exit27 negative that correctly produces aggregate false/host1. Original713, VOD734 and union736 executions stay separately scoped; no new VODHTTP/package/performance pass is inferred. Multiple profiles and exact failure-result rendering remain open; Main/default graph stays OFF. [Reproduction and bounds](../scripts/rust-job-contracts/active-notifications/README.md) preserve failed predecessors and process-local signal scope.

Current packaged checkpoint: [image51](../scripts/rust-integration/verified-checkpoint-image51-1.json) passes twenty scoped candidate groups plus fresh Original import, shipped CLI17/createsuperuser22, selected physical Source/Main4s success/busy/clientloss3cases, two-worker sharing/recovery24 and100 late-history readers on772 committed Rust inputs. Strict checks and558 default test executions pass (692 ignored fixtures);29 owned projects are independently empty. React/LICENSE/runtime credit are preserved. Recovery and the default worker graph stayOFF. [Image48 live measurements](../scripts/rust-live-perf/image48/README.md) remain bound to that historical image; no numerical gains transfer to image51. Full milestones remain open; [reproduction and limits](rust-image51-validation.md) bind this checkpoint.

Current follow-up: [the four-second physical Main observation](rust-vod-four-second-image50.md) is valid but fails parity: Original200/429 versus native503 near three seconds. The [772-input idle-reservation/scanner recovery union](rust-union772-validation.md) passes its scoped source/component gates; image51 now passes the selected packaged Main replay. Full public-counter/timer parity is still open. The [finite JSON formatter fix](rust-finite-json-compatibility.md) is committed with9,368 finite comparisons and512 actual file vectors. Scanner process-loss recovery now explicitly releases its lock after settled work; the inherited-descriptor correction and new service/task cohorts pass. A different prior ten-second SQL-drain miss remains unexplained historical evidence. These remain individual contracts; M0–M8 stay in progress and M9 has not started.

The prior [image42 checkpoint](../scripts/rust-integration/verified-checkpoint-image42-1.json) retains its14 input-storage/79 recovery/calendar and interval-scheduler evidence on674 inputs. Those tests are not re-labeled as image44 executions. Image43's standalone image tests passed, but actual Main returned404 because its image router was omitted; the failure is preserved, and image44 attaches the router and repeats packaged validation.

[Static/HTML progress](rust-static-assets.md) includes27 exact asset files,68 actual Source nginx comparisons,56 prior HTML observations and32 more for the actual Chromium Accept header. The native browser executes the API page's scripts, applies Bootstrap and loads Glyphicons after normal UI login. General HTML/static grammar and full Browsable API workflows remain open.

The read-only classification now repeats once only after proven dropped I/O, retaining physical caller/control ownership and never retrying mutations. The prior canonical41 [capped266](../scripts/rust-redis-restart-contracts/verified-vod-cap-image41-targeted-266.json), [fast267](../scripts/rust-redis-restart-contracts/verified-vod-fast-image41-targeted-267.json), [headerloss268](../scripts/rust-redis-restart-contracts/verified-vod-headerloss-image41-targeted-268.json) and [seek269](../scripts/rust-redis-restart-contracts/verified-vod-header-seek-image41-269.json) targeted replays pass; full public-idle/retired/audit recovery remains unaccepted. Scheduler constructors remain unregistered. The [calendar component](../scripts/rust-job-contracts/CALENDAR-SCHEDULER-NATIVE.md) covers five captured2026e zones,1970–2037 and known numeric grammar; general calendar grammar/zones, default task families and configured Series rules remain open. Recovery and EPG refresh stay OFF by default. [Cached provider-info](rust-cached-provider-info.md) passes its captured cached-only domain. [VOD image/cache routes](rust-vod-images.md) pass their captured domain and physical ownership tests. Stale/forced provider refresh, uncaptured provider-modal/query/image workflows, broader API/browser coverage, per-milestone/full-service performance, soak and cutover remain open. Historical performance reports retain their original image/workload bindings. Image44 makes no new resource-gain claim. The [optimized core comparison](../scripts/rust-core-perf/README.md) passes nine fixed paired trials on its exact reconstructed Source/ELF snapshot: median paired CPU reductions26.1456/73.0244/91.1180% and peak working-memory reductions29.4209/32.0612/48.6231% at1/10/100 readers. All measured fast/slow class byte ratios are1.0. These are core plus commonPG/Redis results, not image44/full-application gains. The [new core functional checkpoint](../scripts/rust-core-contracts/verified-core-native-functional-2.json) matches23 controlled Source/native observations plus five pending-marker steps on691 exact Rust inputs. It passes11 shared-input HTTP and14 input-storage controls,379 default tests and strict workspace Clippy;601 fixture-dependent tests remain explicitly skipped in that default run. Producer finish preserves retained reads, marks publish the next-index sidecar before media, and one pending marker fits beyond a full payload window without increasing payload limits. The [public build and replay instructions](../scripts/rust-core-contracts/NATIVE-TRACE.md) have20 validator controls and no private support prerequisite. Arbitrary failure interleavings remain unqualified. The prior core resource gains remain bound to their archived source/ELF; the later [Core8 rebind](../scripts/rust-core-perf/core8/README.md) passes the changed core's fixed performance gate. M0 remains a prerequisite for formal M1 closure.

The [opt-in VOD idle recovery checkpoint](rust-vod-public-idle-recovery.md) adds49 component controls and four actual Source/native package comparisons over689 exact Rust inputs: normal reopening and fresh provider-limit refusal, each separately anonymous or initially Bearer-authenticated. Sixteen bounded MP4 prefixes decode. Its [parent integration gate](../scripts/rust-integration/verified-vod-idle-parent-1.json) passes378 default tests, strict workspace Clippy and12 packaged regression groups, with599 fixture tests explicitly skipped. It preserves explicit concurrent-follower, worker/connection-header and broader recovery gaps, remains OFF by default, and makes no M4 or performance completion claim.

The [safe native operator checkpoint](../scripts/rust-operator-contracts/verified-safe-operator-37.json) ports two network-reset commands as a separately shipped binary. Its scoped package passes17 typed SQL/cache/notification/Channels cases, eight actual Source/native locked-row and sequence observations, and six resource controls. Existing-notice updates preserve sequence state; network full saves and user update_fields saves retain their different deletion behavior. [Supported grammar, reproduction and limits](../scripts/rust-operator-contracts/README.md) are explicit. Django-wide CLI, other app-ready hooks, plugin/admin behavior and full M8 acceptance remain open.

Necessary next recovery gates are documented in the unimplemented [active-session design](../scripts/rust-redis-restart-contracts/ACTIVE-VOD-RECOVERY-DESIGN.md), [output-profile cache plan](../scripts/rust-redis-restart-contracts/PROFILE-CACHE-RECOVERY-PLAN.md), and [refresh failure/orphan plan](../scripts/rust-job-contracts/EPG-REFRESH-FAILED-ORPHAN-PLAN.md). These plans describe prerequisites and adversarial tests; they are not implementation or acceptance evidence.

The [native index terminal checkpoint](../scripts/rust-job-contracts/INDEX-TERMINAL-RESULTS.md)
adds verified Directory/P0001 failures, tolerant file outcomes and retained Redis
result ownership for both worker paths. Its classifier is test-only and native
diagnostics differ from Python traceback frames. This is M6 component progress;
it does not close the M6 application, recovery or performance gates.

The [cached Redis restart comparison](../scripts/rust-redis-restart-contracts/verified-cached-redis-restart-147.json)
completed successfully as an experiment but **failed the recovery gate**: the
Original cached client recovered, while existing native connections did not.
Reconstructed native connections succeeded. The later
[bounded recovery checkpoint](../scripts/rust-integration/verified-recovery-permission-components-1.json)
adds managed future-operation recovery for TaskLocks, Coordinator and shared
RedisTransport without replaying uncertain commands. A first post-restart call
can still fail; independent live-client/VOD/notification-cache owners and full
application recovery remain separate gates.
The [Original double-failure capture](../scripts/rust-job-contracts/INDEX-DOUBLE-FAILURE.md)
also establishes that a release failure overrides a directory/SQL failure and
does not undo a committed index. Neither observation is a native compatibility
pass or a performance claim.

The [Original database scheduler reference](../scripts/rust-job-contracts/DATABASE-SCHEDULER.md)
passes41 copied-database checks for due state, actual publication, persisted run
counts, reconstruction, preloaded scheduler overlap, rejected publication and11
controlled calendar/timezone reference vectors.
Native scheduling, calendar/timezone boundaries, crash windows, packaging and
performance remain open.

The [VOD category source checkpoint](../scripts/rust-integration/verified-vod-category-source-1.json)
adds application routes sharing the accepted VOD resource limits. Component
interoperability and ordinary regressions pass. The later
[image33 checkpoint](../scripts/rust-integration/verified-checkpoint-image33-1.json)
passes packaged API, actual scoped React workflows and broader regressions.
Its four query/format and seven default-error exclusions remain explicit. It
does not close M5 or establish new performance gains.

The later [owner/category/refresh component checkpoint](../scripts/rust-integration/verified-owners-categories-refresh-components-1.json)
passes strict workspace checks and345 default tests. Its final actual fixtures
match163 category responses with no captured query/format exclusions, all18
default-error/effect cases and393 ordinary VOD responses. Independent cached
client/cache/VOD recovery, retained VOD setup/cleanup and immutable FileJob
dispatch now have bounded component evidence. New canonical packaging, active
session state-loss behavior, additional Redis owners and performance remain open.

The [Original EPG refresh worker reference](../scripts/rust-job-contracts/EPG-REFRESH-WORKER.md)
passes50 real-worker checks for inactive/dummy/missing sources, busy locks,
parser-count deferral and exhaustion. Refresh uses15-second child deferral and a
distinct missing-source string result. Native refresh task transport, successful
imports and application integration remain open.

The refresh reference also covers successful imports, actual child/grandchild
lineage and early ACK/hard-loss behavior. The accepted native service override
retains per-run dispatch through real pending replies, with test-only protocol
and normal-result decoding. A native refresh consumer and result writer remain
separate work; Source-worker interoperability is not a default native consumer.

Current evidence is described in the [local verification guide](rust-local-verification.md).
The separate [image14 API characterization](../scripts/rust-integration/verified-api-resources-image14-1.json) passes three paired resource trials at identical40successfulrequests/second, with14,400 exact JSON responses and zero errors. Median paired CPU and sampled working-memory ratios are0.10483and0.62949. This read-only synthetic API subset does not close browser, authentication, realtime, background-job or full M5/application acceptance.
The unchanged original React suite passed 6,567 tests in pinned Docker packaging,
and the application image now bundles its production build. This does not close
browser workflow parity. Real-schema management coverage reached 405 integrated
checks, plus 11 unique checks from a focused cleanup suite, followed by
[518 combined checks](../scripts/rust-integration/verified-integration-m5-full-518.json)
after stream management was added, then
[655 default checks plus 20 additional proxy contracts](../scripts/rust-integration/verified-integration-m5-startup-relations-1.json), then
[751 default checks including provider read APIs](../scripts/rust-integration/verified-integration-m6-provider-reads-image6-1.json), then
[818 default checks including provider group/filter writes](../scripts/rust-integration/verified-integration-m6-provider-crud-image8-1.json), then
[894 checks including stream-profile settings and Core utilities](../scripts/rust-integration/verified-integration-m6-core-image10-1.json), then
[994 checks including notification reads and personal actions](../scripts/rust-integration/verified-integration-m5-notifications-image11-1.json), then
[1,024 checks and 33 enabled-IP package checks on image13](../scripts/rust-integration/verified-integration-m5-ip-image13-1.json).
[Image14](../scripts/rust-integration/verified-integration-m5-auth-image14-1.json) repeats all 1,024 checks and 33 enabled-IP checks with native catchup management and password-hash upgrades. Its [full catchup replay](../scripts/rust-timeshift-contracts/verified-management.json) passes ten comparison groups, including 31 management cases and actual active/idle admin-stop cleanup. The [tested checkpoint](../scripts/rust-integration/verified-checkpoint-image14-1.json) passes strict workspace Clippy and 188 tests; 147 fixture-dependent tests remain ignored in that default run and have separate explicit reports where executed.
[Image15](../scripts/rust-integration/verified-integration-m5-auth-image15-2.json) repeats the packaged API/authentication and catchup checks with shared authentication throttles and the native WebSocket gateway. The [frozen checkpoint](../scripts/rust-integration/verified-checkpoint-image15-1.json) passes strict workspace Clippy and 195 tests, with 150 external-fixture tests ignored in that default run. Its [packaged gateway gate](../scripts/rust-realtime-contracts/verified-packaged-gateway-image15-1.json) passes 61 checks using two application replicas, 100 sockets, Chromium, a slow reader and held-socket SIGTERM cleanup. Live statistics producers, wired exports, fresh installation and full realtime performance remain separate gates.
[Image16](../scripts/rust-integration/verified-integration-m5-auth-image16-1.json) passes 1,024 API, 33 IP and 34 password checks, including Scrypt. Its [frozen workspace checkpoint](../scripts/rust-integration/verified-checkpoint-image16-1.json) passes strict Clippy and 211 tests (163 fixture-dependent tests ignored in that default run). [Packaged M3U output](../scripts/rust-output-contracts/verified-m3u-image16.json) matches 50 small and three 3.49 MB cases; [live statistics](../scripts/rust-realtime-contracts/verified-live-statistics-image16-1.json) passes 62 packaged cases with actual TS delivery, cross-replica snapshots, WebSockets, a clean 75-frame audio/video prefix decode and shutdown. Fresh native database initialization, admin creation, login and restart preserve data, but the unchanged React Channels workflow exposes missing logos/filter-options reads; full clean-install acceptance remains failed pending a subsequent image.
[Image17](../scripts/rust-integration/verified-integration-m5-auth-image17-1.json) repeats 1,024 API, 33 IP and 34 password checks after [44 source comparisons for logo/filter-option reads](../scripts/rust-integration/verified-browser-deferred-reads-source-3.json). Its [pure-native install/restart gate](../scripts/rust-integration/verified-native-clean-install-image17-2.json) passes all 11 checks: actual unchanged React creates the first admin, logs in, observes all 16 required startup/restart API responses without browser exceptions and preserves database/JWT state through restart. Only the Rust app plus official PostgreSQL/Redis run; there is no Python backend in that test. The [frozen checkpoint](../scripts/rust-integration/verified-checkpoint-image17-1.json) passes strict Clippy and 216 tests, with 166 external-fixture tests ignored in that default run. Populated browser workflows, logo image delivery/mutations, refresh jobs and full operations/performance acceptance remain open.
[Image18 logo APIs](../scripts/rust-integration/verified-logo-packaged-image18-1.json) and the [actual React logo workflow](../scripts/rust-integration/verified-native-react-logo-image18-3.json) add byte-level upload/cache/CRUD coverage and persistent named-volume upload/edit/render/restart/delete checks. [HDHomeRun](../scripts/rust-hdhr-contracts/verified-hdhr-image18-1.json) passes127 HTTP comparisons plus actual generated-lineup playback/decoding and shutdown. VOD/timeshift event transport and the earlier M4 regression also pass scoped packaged gates. The [image18 checkpoint](../scripts/rust-integration/verified-checkpoint-image18-1.json) passes233 Rust tests; [image19](../scripts/rust-integration/verified-checkpoint-image19-1.json) passes237. The incoming-header audit found ACL mismatches in malformed/repeated forwarded headers; image19 closes its captured58-case family, and image20 closes the captured97-case scoped-IP family. Other IP grammar and the legacy rapid-reclose duplicate-event race remain explicitly separate followups. [Image20](../scripts/rust-integration/verified-checkpoint-image20-1.json) passes240 Rust tests with strict all-target Clippy; its sustained VOD/catch-up load preflight also passes. The [playback matrix](rust-m4-performance.md) passes all twelve paired trials under its frozen budgets; it is scoped to VOD/catch-up viewing on image20. No full milestone or new whole-application resource acceptance follows from these gates.
[Image21](../scripts/rust-integration/verified-checkpoint-image21-1.json) passes strict all-target Clippy and249 workspace tests (193 external-fixture tests ignored in that default run). Packaged checks pass1,024 API,33 IP,34 password/hash,127 output-profile contracts plus3 physical-work checks,101 logo cases and 16 unchanged React checks. [XC account/live](../scripts/rust-xc-contracts/verified-xc-image21-1.json) passes112 HTTP cases plus actual H264/AAC playback and shutdown. The [M4 regression](../scripts/rust-timeshift-contracts/verified-image21-regression.json) passes all ten comparison groups, actual VOD/catch-up WebSockets, six archive cases and three consumed-byte boundaries. The rapid-reclose timer fix has [separate host-runtime evidence](../scripts/rust-output-contracts/verified-rapid-reclose-image21-1.json). EPG row/order, extraction-format, importer and cache proofs remain components; refresh jobs and XMLTV output are unfinished. No image21 performance result or full milestone completion is claimed.
[Image22](../scripts/rust-integration/verified-checkpoint-image22-1.json) passes strict all-target Clippy and266 workspace tests (204 external-fixture tests ignored). Its [packaged API gate](../scripts/rust-integration/verified-integration-m5-auth-image22-1.json) repeats1,024 API,33 IP,34 password/hash,101 logo,16 unchanged React,127 output-profile plus3 resource checks and adds141 malformed-body contracts. [Programme reads](../scripts/rust-epg-contracts/verified-program-packaged-image22-1.json) pass37 JSON/HEAD cases; [M3U](../scripts/rust-output-contracts/verified-m3u-image22-helpers.json), [XC](../scripts/rust-xc-contracts/verified-xc-image22-1.json) and the [M4 regression](../scripts/rust-timeshift-contracts/verified-image22-regression.json) also pass. The [rapid-reclose transport gate](../scripts/rust-output-contracts/verified-rapid-reclose-ws-image22-2.json) now matches actual duplicate stop/statistics WebSocket events, audits and Redis cleanup. Separate accepted components cover [bounded owned extraction](../scripts/rust-epg-parity/verified-owned-extraction-55.json), [guide-job reporting in Docker](../scripts/rust-epg-parity/verified-reporter-component-docker-60.json) and [copied-database backup cross-restores](../scripts/rust-integration/verified-backup-cross-restore-3.json). Refresh dispatch/channel parsing, complete guide/output/API behavior, production backup operations, full resource workloads and soak/rollback remain open. Image20 resource results are not image22 measurements.
[Image23](../scripts/rust-integration/verified-checkpoint-image23-1.json) passes strict all-target Clippy and283 default Rust tests (223 external-fixture tests ignored). It attaches [current-programme reads](../scripts/rust-epg-contracts/verified-current-packaged-image23-1.json), [XMLTV output](../scripts/rust-xmltv-contracts/verified-xmltv-image23-2.json), and [job-free backup file operations](../scripts/rust-integration/verified-backup-api-packaged-image23-2.json). The packaged gates pass64 current-programme cases,37 programme reads,47 XMLTV wire/cache cases, and112 backup cases (107 raw exact; five named original204/Content-Length protocol deltas). PriorAPI/browser/authentication, playback, M3U, XC and HDHR regressions pass. XMLTV audit checkpoints explicitly reset the owned2s dedup key before invalidation so backend speed does not change the expected event count. Read-only Compose services setTMPDIR=/data for bounded disk spooling. Refresh/index/backup-create/restore jobs, full grid/custom rules, wider operations, resource workloads and soak/rollback remain open; no full milestone or image23 performance gain is claimed.


[Image24](../scripts/rust-integration/verified-checkpoint-image24-1.json) passes strict all-target Clippy and 288 default Rust tests (232 external-fixture tests ignored), plus 178 tool tests. Its [XC EPG gate](../scripts/rust-xc-contracts/verified-xc-epg-image24-3.json) passes 750 original helper cases over application HTTP and 88 original/native HTTP cases, with explicit admission, canceled-waiter, response-size and shutdown checks. Only independently random synthetic IDs are normalized; a nonzero configured Python hash seed remains an explicit 501 pending its compatibility gate. All prior packaged API, browser, playback, current-programme, M3U, XMLTV, XC, HDHR and job-free backup regressions pass. Guide entity and file-job/logo proofs remain separate components; durable refresh/index jobs and full milestone acceptance remain open.

Image25 frozen workspace validation passes strict all-target Clippy and 296 default Rust tests (241 external-fixture tests ignored). The [packaged guide route](../scripts/rust-output-contracts/verified-grid-packaged-image25-1.json) passes 43 actual original/native real-clock HTTP cases, comparing complete entities, status, MIME and selected headers without input-time or row normalization. The [API/browser/authentication replay](../scripts/rust-integration/verified-integration-m5-auth-image25-1.json), [M4 media/events replay](../scripts/rust-timeshift-contracts/verified-image25-regression.json), [M3U cache roundtrip](../scripts/rust-output-contracts/verified-m3u-image25-2.json), XMLTV, XC and HDHR regressions also pass their scoped packaged gates. Guide hot-cache batching has separate Source and physical-resource evidence. Custom dummy rules, offset-sensitive dummy generation, complete browsable error HTML, durable jobs and wider operational compatibility remain open. These checks do not accept a full milestone or establish a new resource gain.


[Image27](../scripts/rust-integration/verified-checkpoint-image27-1.json) passes310 default Rust tests, strict all-target Clippy and broad packaged API/output/playback regressions. The separate current-file pipeline preserves programme-before-metadata queries, actual relation-lock snapshot timing and ordered serialization effects; its [six-test gate](../scripts/rust-epg-parity/verified-current-order-source-native-106.json) retains the discovered105failure and its correction. [The real Store/file bridge](../scripts/rust-epg-parity/verified-current-real-bridge-source-native-107.json) uses a one-connection pool and proves retained physical ownership. These components remain app-unwired; six malformed-index shapes and relative-path/media migration remain open.

[Image28](../scripts/rust-integration/verified-checkpoint-image28-1.json) passes310 default Rust tests (261 external-fixture tests ignored), strict Clippy and208 Python tool tests. Its targeted XMLTV47/19physical/500-channel replay and M3U50 gates pass. Other production module sources are unchanged from the broadly tested27 image; those reports keep their27 bindings. The bulk-parts reply removes redundant Lua copies without dropping cache guards, TTLs or member bounds. [The isolated cold10 preflight](../scripts/rust-guide-perf/verified-cold-preflight-image28-1.json) passes one pair: CPU ratio0.56918, sampled peak working-memory0.42075, completion-p950.79247 and throughput1.0, with zero errors. The preceding27 preflight failed completion-p951.22203 and remains recorded. The subsequent [complete15-pair matrix](rust-guide-performance.md) passes every frozen gate: Current median CPU/memory ratios0.011773/0.364794; cold1 0.800204/0.467143; cold10 0.580536/0.431112; hot1 0.531753/0.449984; hot10 0.518329/0.456388. All30 measurement phases have60samples and zeroerrors/OOM/resets. This accepts the declared stored Current/XMLTV workload, while full milestones and whole-application performance remain open.

M8 now has actual [Source result/exception contracts](../scripts/rust-job-contracts/verified-original-job-results-2.json), [native publication with the original worker](../scripts/rust-job-contracts/verified-native-source-interop-1.json), and [two delayed-reply/cancellation/drain tests](../scripts/rust-job-contracts/verified-native-job-resources-1.json). Native workers, scheduler and job202/status routes remain disabled. Original authors and contributors retain full application and algorithm credit.

The [retained image23 guide experiment](../scripts/rust-guide-perf/verified-guide-experiment23-recovery-1.json) failed on its tenth pair: hot XMLTV with one viewer had a completion-p95 Rust/Source ratio of 1.16275, above the frozen 1.10 ceiling. The experiment stopped with its raw qualification and measurement evidence retained; the complete matrix was not accepted. The fresh image25 experiment kept the same workload and budgets: Current and cold1 each passed three pairs, then cold10 trial1 failed CPU (ratio1.03209), completion p95 (1.59665) and TTFB. All14 measured phases and 18 original qualification phases remain evidence; the complete matrix is unaccepted. Earlier performance evidence remains bound to its historical image and workload.


Post-image23 [grid helper evidence](../scripts/rust-epg-contracts/verified-grid-window-components-17.json) matches861 original cases, including557 window/date parsing cases; three component tests and strict Store Clippy pass. These helper cases do not themselves accept the later guide HTTP/database/resource or performance gates.

Frozen image13 also passes [23 XC catchup cases](../scripts/rust-timeshift-contracts/verified-xc-routes.json) and [15 direct catchup cases](../scripts/rust-timeshift-contracts/verified-direct.json). Shared settings-cache defaults have a separate [18-case source overlay proof](../scripts/rust-integration/verified-ip-default-map-overlay-3.json); this overlay is not part of image13's immutable bytes.
The [bounded cache/database bridge](../scripts/rust-ingest-parity/verified-cache-batch-pipeline-20.json) and [fenced task locks](../scripts/rust-ingest-parity/verified-task-locks-23.json) add ingestion component evidence without enabling refresh jobs. [Original Python/two-Rust-process ChannelsRedis interoperability](../scripts/rust-realtime-contracts/verified-channels-interop-1.json) passes. The packaged WebSocket transport and browser delivery now pass the image15 gate above; producer coverage and broader browser workflows remain open.
Two-replica TS probes
passed shared demand, global stop and owner-crash cleanup with strict decoding.
The earlier Rust image needed 87.1 seconds for owner-crash cleanup; the new image
needed 26.6 seconds in the same functional probe, with all six captures decoded.
This is an observed cleanup-latency comparison between partial Rust images,
separate from CPU and memory acceptance. Packaged React navigation, production
assets, JSON API fallbacks, and SIGTERM with unread media and incomplete HTTP
headers also [passed](../scripts/rust-integration/verified-runtime-shutdown-1.json).
The process exited successfully in 12.14 seconds and drained the channel's
owner, clients, metadata and input index before closing retained connections.

The cold initialization capture established that the unchanged source can wait
about 50 seconds, then return HTTP 200 with one 188-byte TS error packet; the
earlier Rust image returned JSON 502/504. The new source-bound image includes
four-full-chunk readiness, live positioning after readiness, and native cold
error packets. All three default-timeout cold cases
[matched](../scripts/rust-integration/verified-cold-wire-source-compat-1.json).
User/provider capacity and invalid
credentials are distinct pre-stream JSON errors. Source-ready state also requires
four buffered chunks. A later metadata-before-response ordering fix passed
owner and follower real-Redis tests and is queued for its fresh Docker gate.

The corrected same-channel [native TS characterization](../scripts/rust-integration/verified-native-source-compat-matrix-3.json)
completed all 18 measurements and decoder priming checks at 1/10/100 viewers,
with zero delivery/observer errors. Paired median Rust/source CPU ratios were
0.296/0.186/0.0673 and container working-memory ratios 0.825/0.846/0.778.
The totals include backend, PostgreSQL, Redis and origin. This is a warmed,
partial-backend characterization: browser/realtime/event equivalence, many-channel
and slow-reader resource gates remain open. A [single ten-channel diagnostic](../scripts/rust-integration/verified-many-retention-diagnostic-1.json) delivered valid media with lower CPU, but failed the memory budget (Rust/source 1.0095); it establishes no accepted many-channel gain. After bounded local-cache and historical-read changes, the [image11 three-pair comparison](../scripts/rust-integration/verified-hot-cache-many-channel-image11-1.json) passed all six measurements at ten channels/viewers: median CPU ratio0.2213, total working-memory ratio0.8240 and throughput ratio0.9890. This later native-playback slice result does not replace the failed evidence or accept browser/realtime/full-application parity. It does not accept a complete
milestone or full-application performance gain.

Frozen native timeshift/VOD comparison also
[passed](../scripts/rust-timeshift-contracts/verified-compose.json): 24 API/error
cases, six archive media/lifecycle scenarios, strict audio/video decoding and
literal source profile aliases. Positive shared timeshift admission is a later
frozen-image follow-up. M6 now has [2,283 component cases under the original runtime](../scripts/rust-ingest-parity/verified-preparation-original-runtime-14.json) and [six stream-batch plus eleven group-sync database scenarios](../scripts/rust-ingest-parity/verified-groups-batch-sql-13.json), including the parser-to-database pipeline; see [ingestion gates](rust-ingestion.md).

The core Compose run has passed, the load-harness Compose selftest has passed,
and Python baseline runs have passed (48 focused tests and a separate broader
526-test proxy suite, followed by all 3,362 tests of the unchanged upstream suite; see
[full baseline evidence](../scripts/rust-baseline/verified-full-baseline.json)). Static source discovery and
runtime discovery are available; the runtime resolves 592 Django routes and 246
OpenAPI paths/362 operations, with 58 schema warnings and 66 graceful-fallback
error diagnostics that omit views with uninferred serializers. None of those discoveries is complete executable
contract coverage. A measured TS helper comparison is component-only. Full-stack
playback baselines, frozen scenario budgets, decoder checks, complete M1 gates,
and the required application integration remain outstanding.

Further implementation evidence (acceptance remains open): the Rust HTTP candidate
has decoded 75 frames of actual proxied MPEG-TS through the migrated PostgreSQL
fixture, authenticated Django PBKDF2 passwords, accepted Django access/refresh
tokens, and issued JWTs accepted by Python SimpleJWT. The asynchronous runtime now
leases owned HTTP frames so payloads remain accounted until their final owner
drops. Redis lease tests exercise atomic provider limits and fenced ownership;
HTTP integration of those leases has passed targeted tests. Further real-Redis tests
passed for viewer admission, byte-owner lifetimes, immediate client stop, HLS token
remint/touch/release, cold reconnect, and stale output/channel fencing. These are
component and vertical-slice checks, not completed milestone acceptance.

The HLS segmenter passes 40 unchanged upstream tests through an RPC adapter and
2,145 exact operation comparisons. Generated segments passed FFmpeg decoding.
The gateway and native VOD implementation are still under integration. The first
full resource matrix was stopped because its frozen candidate lacked legacy
per-viewer telemetry; its partial results establish no accepted performance gain.
See [matrix disposition](../scripts/rust-playback-baseline/prototype-matrix-halt.json).

Baseline load findings must remain separate: a 100-viewer cold burst failed twice,
including with fresh web workers and isolated Redis reset. A warmed 100-viewer
fanout passed with a recorded four-second warmup ramp. The database-pool deadlock
explanation is a hypothesis, not an established cause. See the preserved
[failure evidence](../scripts/rust-playback-baseline/cold-reopen-evidence.json).
Warmed fanout does not close the cold-start acceptance gate. Short load-smoke
measurements include startup catchup and are not accepted performance savings.

M3/M4/M5 can proceed concurrently after M2, using separate owned modules and
contract fixtures. M6 can proceed once its M5 models and API contracts stabilize.
Independent contract discovery, fault-fixture development, and test porting can
run throughout. Integration stays sequential: each change lands only after its
shared contracts and prerequisite gates pass.

## Evidence and performance rules for every milestone

For each run preserve: tested Git commit and dirty patch, Python baseline commit,
image IDs/digests, Rust/Python/tool versions, Docker/Compose versions, kernel and
architecture, CPU/RAM and container limits, scenario/configuration, fixture
hashes, warmup/measurement duration, raw samples, summarized results, logs,
exit codes, and declared normalizations. Store reports in an ignored local
artifact directory and keep small sanitized result summaries in version control.
Never record provider passwords, tokens, or real subscriber traffic in fixtures.

Compare a pinned Python build and a Rust build using the same deterministic
fixture source, equivalent seeded datasets, profile, output format, viewer load,
and resource limits. Run sequentially on the same machine; simultaneous backends
compete for resources and invalidate a clean comparison. Use isolated databases,
Redis namespaces, and volumes to prevent one implementation affecting the other.
Record at least three measured repetitions after warmup; alternate backend order.
Treat noisy or incomplete runs as inconclusive and rerun after finding the cause.

Measure the total service group (API/proxy, job workers, Redis, database, FFmpeg,
and compatibility subprocesses) and each component. Include CPU seconds per
wall-clock second, memory current/peak, memory after disconnect/cleanup, startup
p50/p95/p99, successful output bytes and bitrate, errors, upstream connection
count, and media process count. Record cgroup memory and CPU where available;
`docker stats` samples alone may miss short-lived peaks and omit nested child
attribution. Retain metrics-collection capability and failures in the report.

Report relative change as `(Rust - Python) / Python * 100%`; lower CPU/RAM/latency
is better. Normalize CPU by successfully delivered media GiB for playback and by
completed objects/requests for jobs/APIs. Do not claim savings when Rust delivers
less media, drops viewers, skips validation, or performs less background work.
A Rust-only measurement is a useful characterization, not a measured gain.

[The unfrozen budget template](rust-performance-budgets.example.json) captures
required fields and intentionally cannot pass acceptance. The separate
[HTTP harness budget template](rust-harness-budgets.example.json) is consumed by
`scripts/rust-perf/milestone.py` for strict repeated-pair resource checks; that
subset does not validate media or cleanup and cannot pass a full milestone alone.
M0 freezes numeric budgets in a scenario manifest before implementation results
are used to choose them. Required fields: minimum valid media throughput,
maximum error count, startup p95 ceiling, CPU ratio ceiling, memory ratio ceiling,
cleanup deadline, and allowed post-cleanup growth. For functionality-only
milestones set CPU/memory non-regression budgets; for streaming milestones set
an explicit improvement target based on the observed baseline and noise floor.
The report must fail if required fields, baseline samples, or sufficient successful
work are absent. Until budgets are frozen and measured, performance is pending.
The final goal requires a material total-service improvement under viewing load,
not just improvement in one function or process.

Minimum scenario matrix:

| Scenario | Purpose |
| --- | --- |
| Idle after startup and after previous playback | Base footprint and leak detection |
| 1, 10, and 100 viewers on one channel | Shared ingestion and per-viewer scaling |
| 10 distinct channels, then configured capacity | Per-channel memory/CPU and slot limits |
| Slow reader plus fast readers | Backpressure, bounded memory, unaffected fast viewers |
| Connect/disconnect and reconnect churn | Slot accounting, sockets, tasks/process cleanup |
| Upstream stall, failure, source change, and recovery | Timeouts, failover, discontinuity, retry behavior |
| Import/EPG refresh during playback | Background contention and viewer latency |
| Sustained playback followed by disconnect | Retention bound, long-run growth, cleanup |

## Local Docker Compose contract

The migration stack lives at `docker/compose.rust.yml`, the Rust validation image
at `rust/Dockerfile`, and performance tooling under `scripts/rust-perf/`. Invoke
Compose with `docker compose -f docker/compose.rust.yml ...`; consult that file and
the harness documentation for implemented profiles and commands. The complete
stack described below is an acceptance requirement, not a claim that all its
services already exist.


Use a migration-specific Compose stack alongside the production definitions;
production deployment defaults must remain usable throughout the migration.
The final harness needs services/profiles for a fixture origin, PostgreSQL,
Redis, pinned Python baseline, Rust candidate, and a test/load runner. M0 may
start with a smaller Rust-only developer stack, but that does not pass baseline
or parity acceptance. Add only locally generated media and mocked providers;
never use a paid provider as the benchmark source.

A single documented runner must validate `docker compose config`, build images,
start dependencies, wait on health checks with a finite deadline, seed isolated
data, execute assertions, collect reports/logs, and propagate test failures as a
nonzero exit. Use unique project names and dedicated disposable volumes. Cleanup
must address only that test project's resources. Support retaining failed runs
for inspection. Bind exposed test ports to loopback. Container health must test
readiness for the milestone rather than merely that a process exists.

Execute the same suite from a fresh checkout/clean volumes and an existing-data
upgrade. Test graceful SIGTERM and forced worker death. Restart tests must verify
no orphan media subprocess, stale ownership lease, leaked profile slot, or
unreachable channel survives the documented recovery deadline. Do not route
active real channels to Rust until M2's ownership and rollback gate passes.

## M0 — baseline and executable contracts

* Install Rust/Cargo, pin a reproducible toolchain, and run workspace tests,
  formatting, and lint checks locally and in the Rust image.
* Resolve Django URLs including DRF router-generated actions, not only literal
  `path` calls. Snapshot OpenAPI and inventory non-OpenAPI URLs from
  `dispatcharr/urls.py`, `apps/output/urls.py`, `apps/hdhr/urls.py`, and proxy URLs.
* Build a contract register: unique ID, source handler/test, method/path or event,
  role, setup fixture, assertions, owning milestone, coverage state, and result.
  Inventory environment options, management commands, schedules, schema,
  plugin context APIs, and admin workflows separately from HTTP routes.
* Generate deterministic TS, fMP4, HLS, and seekable VOD fixtures plus malformed,
  stalled, redirected, ranged, and failure-injected upstream responses.
* Run the pinned Python tests using the existing backend test-discovery/bootstrap
  workflow (`scripts/ci_backend_test_labels.py`, `scripts/ci_bootstrap_backend.sh`)
  in its supported image. Preserve pre-existing failures separately; do not
  silently accept failures in contracts targeted by the rewrite.
* Capture baseline scenarios above and freeze per-scenario performance budgets.

Pass: another developer can reproduce contracts and measurements using Compose;
no baseline or tool unavailability remains hidden behind a passing status.

## M1 — streaming core

The same 33 streaming-core and runtime library tests passed after both
crates were recompiled at the exact restore-enabled `976f2867` snapshot.
All 1,496 Rust inputs matched its package. [Exact 976f host receipt](../scripts/rust-integration/m1-976f-bounded-core-host.json)

The exact `a785a2b7` package passed all 17 stream-core and 16 stream-runtime
library tests after recompiling both crates. All 1,496 Rust inputs matched
the package source map. The Compose and M0 prerequisite gates retain their
separate scope. [Exact a785 host receipt](../scripts/rust-integration/m1-a785-bounded-core-host.json)

The packaged plugin-header successor at `8b0c76ca` passed 17 streaming
core and 16 runtime library tests on the host. All 1,494 Rust source
inputs matched its exact package. The tests cover TS alignment, atomic
write refusal, shared viewer payloads, retention, cancellation and
notification behavior. M1 still requires the integrated Compose scope
and its M0 prerequisite.
[Host test receipt](../scripts/rust-integration/milestone-batch113/stream-core-runtime-33.json)


Add TS alignment across arbitrary input chunk boundaries, truncated-packet
handling, time-based startup positioning, lag recovery, discontinuity stamping,
bounded retention, independent viewer cursors, notifications without lost
wakeups, cancellation, and shutdown. Define the adapter from Python's
last-consumed cursor to Rust's next-to-read cursor explicitly.

Port expectations from `test_ts_discontinuity.py`,
`test_buffer_discontinuity.py`, and relevant segmenter/buffer tests under
`apps/proxy/live_proxy/tests`. Add concurrent slow/fast reader and idle expiry
cases. Bound retained media, queued transport references, channel count, and
in-flight client data; a ring byte cap alone is insufficient.

Pass: deterministic core tests and container execution pass; memory remains
bounded under slow readers; core benchmark compares equivalent Python and Rust
work. Label these as core results, distinct from full playback gains.

## M2 — authenticated TS playback vertical slice

Implement route/channel resolution, existing authentication and authorization
semantics, pool/profile limits, upstream HTTP delivery, channel ownership/leases,
viewer registration, stats, stop, failure/keepalive deadlines, and release-once
cleanup. Preserve anonymous capability behavior where the baseline permits it;
do not globally add or remove authentication.

Use existing `test_stream_ts_client_registration.py`, `test_start_race.py`,
`test_ts_generator_release.py`, `test_metadata_slot_release.py`,
`test_slot_release_helpers.py`, `test_process_tree_cleanup.py`,
`test_health_reconnect.py`, and `apps/proxy/tests/test_stream_limits.py` as
contract seeds. Test two workers contending for the same channel, source death,
client cancellation, and Rust rollback. The unchanged frontend must play a seeded
channel and show correct viewer stats.

Pass: Compose delivers decoder-valid TS with expected auth and ownership;
repeated connection/disconnection returns resource counts to baseline; measured
single-channel fanout meets the frozen performance budgets.

## M3 — live formats, profiles, and failover

Implement profile transforms and output sharing, fMP4 init/fragment delivery,
native HLS segmentation/playlist/segment/capability semantics, session stop,
TTL refresh, and source switching. Preserve supported FFmpeg/VLC/profile
arguments and accounting. Count external process costs in reports.

Port `test_hls_segmenter.py`, `test_hls_views.py`,
`test_hls_playlist_waiters.py`, `test_hls_cold_start_lua.py`,
`test_hls_manager_discontinuity.py`, `test_hls_session_stop_api.py`,
`test_output_ttl_refresh.py`, `test_fmp4_remux_interleave.py`,
`test_failover_retry_window.py`, and profile/stream-switch tests.

Pass: player/decoder checks cover each format and switch; many viewers share the
expected pipelines, with no new process per equivalent viewer; Compose fault
runs and per-format performance budgets pass.

## M4 — VOD and timeshift

Preserve HTTP Range, Content-Range, HEAD if supported, seeking, extension and
content-type behavior, redirect validation, alternate provider selection,
connection limits, profile restrictions, XC movie/series IDs, timeshift URL
formats, and cleanup. Cover missing IDs and upstream errors exactly.

Port `apps/proxy/vod_proxy/tests`, `apps/timeshift/tests`, and corresponding
output tests. Pass: compare headers/statuses/body ranges, decoder-seek behavior,
concurrent limits, failure recovery, and post-session resource counts in Compose;
VOD/catchup throughput and memory/CPU meet budgets.

## M5 — accounts, settings, and channel APIs

Port account setup/login/proxy-login/logout, API keys, JWT refresh, user/group
permissions, per-user app data, settings, profiles, channels, groups, logos,
filters, sorting, pagination, validation, and defaults. Preserve existing hashes,
IDs, constraints, timezone handling, trusted-proxy rules, and PostgreSQL/SQLite
behavior. Enforce the same role-specific visibility and errors.

Pass: contract register covers each router action and role; existing DB fixtures
work without resetting passwords or IDs; unchanged React workflows pass against
Rust; API load and idle-footprint budgets pass. Keep schema migration ownership
single and changes additive during coexistence.

[Channel Batch Radio and Standard User Level acceptance](../scripts/rust-integration/m5-channel-batch-radio-level-7ccbb7a2.json) passes one ordinary administrator PATCH and two ordered detail reads per backend on packaged Rust `7ccbb7a2` and pinned Original Dispatcharr. Both channels begin from the same scoped PostgreSQL state. React’s string Standard User level becomes integer 1. The managed channel keeps its raw provider radio value and receives a false radio override. The manual radio value is changed directly, and both effective values are false. Existing group and number overrides, clocks, provider ownership, stream links and unrelated scoped rows are preserved. All four role generations retire and owned inventories are empty. Additional Channel Batch fields, full M5 and performance measurements remain open. The original Dispatcharr authors and contributors retain full credit.

[Mixed Channel Batch group and mature-flag acceptance](../scripts/rust-integration/m5-channel-batch-group-4faa831b.json) passes one ordinary admin bulk PATCH and two ordered typed detail reads per backend on packaged Rust `4faa831b` and pinned Original Dispatcharr. The two backends start with equal scoped PostgreSQL state. The managed row keeps its raw group and prior number override while its group override changes. The manual row changes its raw group, both rows store the mature flag, and the destination receives one custom provider association while retaining its existing provider. Existing row clocks, links and unselected scoped rows remain unchanged. All four owned role generations retire and all owned inventories are empty. The port uses set-based writes with 100-row scalar and 200-row override batches. The physical run covers two channels. Broader batch fields, larger physical selections, resource measurements and full M5 remain open. Original Dispatcharr authors and contributors retain full credit.

## M6 — imports, guide, matching, and jobs

Port M3U/Xtream/VOD imports, XMLTV/EPG, scheduled refresh, progress updates,
matching/search, filters, deduplication, partial-failure preservation, retry and
cancellation semantics. Inventory Celery/beat tasks before replacing them;
in-memory timers do not preserve durable scheduling and retry behavior.

Seed contracts from `apps/m3u/tests`, `apps/epg/tests`, `apps/vod/tests`,
`apps/m3u/tasks.py`, `apps/epg/tasks.py`, `apps/epg/sd_tasks.py`, and
`apps/vod/tasks.py`. Include compressed input, empty/incomplete upstream data,
restart during import, schedule timezone boundaries, and DB recovery.

Pass: imported objects and retained details match baseline fixtures; durable
jobs survive interruption without duplication; guide/import throughput, peak
memory, and playback-during-refresh budgets pass in Compose.

## M7 — protocols, realtime, and integrations

Port M3U/XMLTV/Xtream outputs, HDHomeRun endpoints/discovery, stats payloads,
WebSocket messages and connection auth, events, notifications, and `apps/connect`
behavior. Preserve response shape and wire-format details used by real clients.
`dispatcharr/consumers.py` restricts connection telemetry and admin notifications;
cover unauthorized, standard, and admin sockets rather than only happy paths.

Pass: `apps/output/tests`, HDHR tests, connection/event tests, and WebSocket
contracts pass through actual container networking. M3U/XMLTV generation,
WebSocket fanout, and integration delivery performance meet budgets.

## M8 — operations and remaining compatibility

Implement compatible backup/restore, supported admin workflows, logs, static/media
handling, CLI/management operations, environment and TLS options, upgrades,
observability, and Python plugin/provider/matching dependency strategy.
Existing Python plugins may import Django APIs and invoke Celery. An isolated
interpreter without those APIs is not compatible. Keep a tested compatibility
runtime until equivalent behavior exists; account for its memory and CPU.
No plugin or admin behavior may disappear under a full-parity claim.

Pass: fixture plugins exercise documented settings/actions/context/imports;
backup round trips preserve IDs/hashes/media references; restored data works
against both implementations; Compose covers supported configurations, SQLite
and PostgreSQL, startup and graceful shutdown. Report full-service idle and
active resource totals including compatibility workers.

## M9 — release and cutover rehearsal

All contract IDs must be covered by passing executable tests or documented as
unresolved; unresolved required behavior blocks full parity. Run all backend
contracts, unchanged frontend workflows, fault scenarios, fresh install and
upgrade, and a sustained soak (at least two hours, including refresh activity)
with post-soak cleanup. Inspect resource growth and decoder errors.

Pass: every preceding gate passes on the release images; CPU/RAM improvement
holds for the complete service group under viewing load; no functionality is
removed to obtain it. Rehearse rollback on copied data and publish exact image,
configuration, migration, restore, and rollback steps. Production deployment is
a separate action; local acceptance does not itself alter the live service.

[Image24](../scripts/rust-integration/verified-checkpoint-image24-1.json) adds the tested XC short/simple guide actions. It passes750 captured helper cases through actual packaged HTTP plus88 original wire cases, with SQL-wait cancellation/admission, response bounds, generated-ID configuration and shutdown gates. Strict workspace Clippy and288 default Rust tests pass;232 external-fixture tests remain separately tracked. All eight API/authentication/backup suites,64 current-programme reads,37 programme reads, XMLTV47/cache/audits, XC112, HDHR127, M3U50, all ten VOD/timeshift groups and playback WebSocket/byte boundaries pass on this same image. Reviewed captured tools pass178 tests. Guide22 complete entities and automatic-logo/channel/local-file job components have separate accepted component proofs; application dispatch remains open. Expanded7189-date parser fixes are post24. No complete milestone or image24 performance gain is claimed.

Post-image24 [compiled Python/Django date handling](../scripts/rust-epg-contracts/verified-grid-datetime-components-25.json) now matches7,189 expanded original vectors and regresses the861 helper cases in a fresh frozen component proof. This correction is outside image24. Full guide attachment and performance remain separate gates.

[Image29](../scripts/rust-integration/verified-checkpoint-image29-1.json) packages committed revision `2136b348` without sibling work in progress. The guide metadata fix now passes the actual relation-lock schedule that failed its first comparison,22 metadata/stored and22 complete-entity component cases,40 HTTP cases with physical ownership controls, and43 actual original/native packaged network cases. Strict full-workspace Clippy and311 default tests pass (264 external-fixture tests intentionally ignored). The initial validation invocation omitted external test datasets from the application-only archive; restoring823 exact committed scripts/dataset inputs fixed compilation without changing Rust. The new Create worker retains its separate fixture-only24 execution/two physical-owner scope. Image28's accepted performance remains bound to image28; no gains or broader image28 endpoint/media results are reassigned to image29. Native path/index/VOD work and full milestone acceptance remain open.

[Image30](../scripts/rust-integration/verified-checkpoint-image30-1.json) contains the bounded guide waiting fix and the previously accepted path/dispatcher components at committed `916e4e50`. It passes313 default tests (269 external-fixture tests intentionally ignored), strict all-target workspace Clippy and43 packaged grid network contracts. The [large unstaggered grid](../scripts/rust-guide-perf/verified-grid-functional-image30-1.json) now succeeds for all ten Source/native callers with exact2250-row typed/order JSON, closing image29's retained eight503 responses. Two active jobs,192MiB workspaces and512MiB Docker backend limit are unchanged; ten bounded owned requests may wait with cancellation/expiry and physical shutdown controls. The separate GRID resource policy is frozen before measurements; no new gains or whole-milestone acceptance follow from this checkpoint.

The [complete image30 GRID resource matrix](../scripts/rust-guide-perf/verified-grid-experiment-image30-1.json) now passes all six pairs: CPU decreases 71.6%/73.3%, sampled peak group working memory 60.0%/59.6%, completion p95 51.1%/52.1% for one/ten paced admin consumers, with unchanged throughput. All 12 measured phases meet unchanged budgets with 60 samples each and zero errors/late completions/OOM/counter resets. Source-only qualification rejects the ten-consumer1 second rate, selects2 seconds, and retains all ten late qualification completions. Public840 samples/2445 timing rows cover all 20 phases. [Results, methodology and plot](rust-guide-performance.md#stored-react-guide-grid--image30) preserve the separate image binding. This accepts the named stored-grid checkpoint, not provider ingestion/jobs, full browser workflows,100+viewers, plugins or complete M6/M9.


[Image31](../scripts/rust-integration/verified-checkpoint-image31-1.json), committed `db532d02`, attaches ordinary movie/series/episode reads and retained cached authentication. Strict whole-workspace Clippy and 322 default tests pass; 282 external-fixture tests remain excluded from the default run. Its Docker regressions pass all 1,024 default API checks, 38 proxy-auth checks, 34 password-hash checks, 95 user-agent checks, 130 output-profile checks and 43 exact guide network comparisons. The unchanged React UI passes 16 fresh-install/admin/login/logo/restart checks with native PostgreSQL/Redis initialization.

[Packaged ordinary VOD reads](../scripts/rust-vod-api-contracts/verified-vod-rest-packaged-image31-1.json) match 393 original HTTP replies, including selected security headers, and ten simultaneous requests all succeed. The remaining 199 captured original cases are outside this adapter and are not parity passes. Four separate Source/native VOD and timeshift media preflight phases match 1 MiB consumed prefixes, pass strict audio/video decoding, observe WebSocket events and release provider counters. These checks preserve their stated scope; categories, unified reads, provider actions/refresh, artwork and full VOD browser flows remain open. No image30 or image28 performance result transfers to image31, and no complete milestone is accepted by this checkpoint. All application and algorithm credit remains with the original authors and contributors.


[The current-file/index component checkpoint](../scripts/rust-integration/verified-current-index-components-1.json) combines accepted frozen modules and passes strict whole-workspace Clippy plus 325 default tests (293 external-fixture tests excluded). Actual original/native HTTP checks now preserve failed publication and six malformed-index HTTP 500 responses. The one-connection Store/file bridge and default index configuration regressions pass. The copied media/data rehearsal verifies 11 path cases and 58 checks as UID 65532 on a read-only root, retaining untouched original files for rollback.

The bounded index intake/ETA worker supports retained physical command ownership, warm restoration, hard-loss reservations and duplicate-ID execution. Actual duplicate tests prove execution repeats while previous SUCCESS metadata and its TTL remain unchanged. These are component gates: no default shared-queue consumer, application current-file hook, automatic visibility recovery, exception writer, complete scheduler or production cutover is accepted. Original Dispatcharr and Celery/Kombu authors retain their respective application and transport algorithm credit.


[The actual HTTP-to-index-driver gate](../scripts/rust-epg-parity/verified-current-http-index-driver-134.json) now passes against the same runtime modules as `d1329a65`: the first authenticated request returns Original's Parsing JSON and durably publishes the initial job; the supervised native worker builds the PostgreSQL byte-offset index; the second request matches Original's programme JSON. Queue, unacked reservations, file locks and owned lookup resources drain to zero. This adds a tested component path with isolated broker/result databases; it does not enable a default consumer or close mixed-family, visibility-recovery, failure-result or production-volume gates. The earlier133 copied-database fixture setup failure remains recorded.


[Image32](../scripts/rust-integration/verified-checkpoint-image32-1.json) is built from committed `d1329a65`; all 521 Rust source files match the exact workspace that passed strict Clippy and 325 default tests. Its unchanged default stored-Current path passes 64 actual original/native network comparisons. The optional file/index callback remains app-unwired. This is a packaged regression checkpoint, with no transferred image28/image30 gains and no full migration acceptance.


[The complete ordinary VOD list API experiment](rust-vod-performance.md) now passes all six pairs on exact image31. Median paired CPU decreases are 58.0%/61.7%, sampled peak group working-memory decreases 35.3%/35.2%, and completion-p95 decreases 57.7%/58.7% for one/ten virtual readers, with unchanged throughput. Each reader alternates movie/series page24 requests once per second. All 12 measured phases have 60 samples and zero errors/late/OOM/resets; 3,960 measured responses match the original. Public evidence retains 840 samples and 4,258 timings across all 20 phases. This accepts that API workload only; provider actions, full browser/media workflows and complete M4/M5/M6/application acceptance remain open.


[The worker payload/visibility component checkpoint](../scripts/rust-integration/verified-worker-payload-components-1.json) passes strict whole-workspace Clippy and 328 default tests (300 external-fixture tests excluded). Its observed failure-payload relay passes 36 actual original-reader checks plus two delayed-reply/wrong-type resource tests, preserving existing SUCCESS metadata/TTL and pubsub rules. Three separate real-Redis visibility tests verify bounded ordering, mutex/score/owner fences and retained physical replies. Both modules remain test-only: generated native traceback equivalence, actual worker exception classification/attachment, automatic polling and Source's full eligible-set sweep remain unaccepted.


[Image34](../scripts/rust-integration/verified-checkpoint-image34-1.json), built from `a84d4aca`, passes all ten packaged regression groups: API, authentication/proxy, settings profiles, stored guides, playback preflight, fresh installation and scoped unchanged React category workflows. All 550 Rust files match the workspace with 345 passing default tests and strict Clippy (333 external-fixture tests intentionally excluded there). The category gate compares 393 ordinary and 163 category HTTP responses with zero captured exclusions, plus 18 default-error/effect comparisons; the previously retained four query and seven default-error gaps are closed in this image. This is scoped regression evidence, with no full milestone acceptance or transferred historical performance gains.

The [actual active-VOD Redis state-loss comparison](../scripts/rust-redis-restart-contracts/verified-active-vod-image34-166.json) is a valid **failed migration gate**: the original continues a matching 278,528-byte prefix and reopens the same session with HTTP200, while image34 ends after 65,536 bytes and reopens with HTTP503. Both processes remain running and both sampled prefixes match the origin. Full-file decoding and event-detail parity are not established by this test. Recovery must preserve ownership fences and needs a new passing gate. The [Redis lifecycle inventory](rust-redis-lifecycle-inventory.md) also identifies an additional output-profile cache connection and unverified subscription/cache-generation boundaries; prior six-owner tests do not establish application-wide recovery.


[The complete image34 category metadata experiment](rust-vod-category-performance.md) passes all six pairs at the Source-qualified one-second interval for one reader and five-second interval for ten readers. Median paired CPU reductions are 70.4%/70.7%, sampled peak group working-memory reductions 60.4%/61.6%, and completion-p95 reductions 65.0%/65.7%, with throughput ratios 1.000. All 1,080 measured responses match the exact Original bytes. Public numerical evidence retains 840 samples and 1,601 timings across all 20 phases, including the two pacing-infeasible Source qualifications. Root independently checked raw cgroup deltas, memory sums, request traces, fixed gates and owned Docker cleanup. This accepts the category-list workload on the exact a84d4aca image34; it does not transfer gains to new source, other VOD routes or the complete application. Active VOD recovery and complete milestones remain open.


[The refresh publication/immediate-worker component checkpoint](../scripts/rust-integration/verified-refresh-publication-worker-components-1.json) passes strict workspace Clippy and 349 default tests (341 external-fixture tests intentionally excluded there). Five actual copied-database/Redis replays use the same 557 captured Rust-tree inputs: publication124, immediate refresh worker76, ordinary constructor/index19, ETA20 and duplicate24, with separate physical reply/worker controls. The fixture worker runs real FileJob services, publishes typed children and writes observed normal results that Original AsyncResult reads correctly; duplicate execution preserves previous SUCCESS bytes/TTL. The worker remains test-only, with no default application intake or public202 authority. Orphan cleanup, unclassified Failed outcomes, native refresh ETA/visibility, scheduling and full ingestion/performance remain separate gates.


[Original orphan-schedule cleanup](../scripts/rust-job-contracts/verified-original-epg-orphan-cleanup-1.json) now has 79 copied-database checks across eleven direct-helper cases and nine actual worker cases, with pinned Django/django-celery-beat implementation capture. It establishes three separate commit boundaries, SET_NULL references, marker failure effects and the normal missing-source result even when cleanup fails. Worker SQL tracing is explicitly unavailable after the Source resets its connection; direct-helper queries establish SQL order. This is an Original reference checkpoint, with native FileJob integration and operational gates tracked separately.


[The additional Source VOD identity observation172](../scripts/rust-redis-restart-contracts/verified-active-vod-source-identity-172.json) proves that the old body continues after state loss while same-ID reopen creates a second provider response and fresh public metadata with active/request/provider counts of one. Both bodies then deliver their expected bytes concurrently. Recovery therefore needs separate old/new local generations; eager reconstruction or recounting all old bodies would change observed behavior. This adds Original reference evidence, not a native recovery pass.


[The profile/lease owner checkpoint](../scripts/rust-redis-restart-contracts/verified-profile-lease-composite-1.json) now passes strict workspace Clippy and 349 default tests (359 external-fixture tests intentionally excluded there) on 559 captured Rust-tree inputs. Its actual fixtures pass all 127 profile HTTP/SQL/cache cases, same-service restart and three AUTH-opening controls, forty lease physical controls, ten provider/credential fences, acquisition cancellation and the 76-check refresh-worker regression. Profile cache generations reconnect without replaying uncertain commands; physical lease mutations retain their real admission until replies settle. The original image34 cache failure and lease169/opening negative tests remain recorded. This closes those bounded source defects, with no new packaged/performance or full application recovery acceptance.


[The integrated orphan cleanup checkpoint](../scripts/rust-job-contracts/verified-epg-orphan-integration-composite-1.json) passes strict whole-workspace Clippy and 351 default tests (360 external-fixture tests intentionally excluded there). Its exact 560-input Rust tree passes eleven helper comparisons, nine actual Original-producer/native-FileJob/Original-result cases, held SQL caller/pool controls and the 80-check refresh regression; ordinary constructor19, ETA20 and duplicate24 also pass. FileJob now performs the proven three-phase cleanup before returning the observed missing-source string, including known caught helper failures. The fixture worker no longer refuses that orphan case. Default application intake, scheduler/HTTP202, unclassified failure outcomes, broader concurrency and complete M6/performance acceptance remain open.


Source restart observations [176](../scripts/rust-redis-restart-contracts/verified-active-vod-source-old-progress-176.json) and [177](../scripts/rust-redis-restart-contracts/verified-active-vod-source-old-close-177.json) add two compatibility requirements: an old body can update the newer same-ID public byte count, and closing that old body can zero public/provider counts and delete metadata while the younger body continues without EOF. Native recovery must separate true local body ownership from those observable public effects and apply any delegation through the current verified owner. These are Original reference gates, not a native body-recovery pass.


[Image35](../scripts/rust-integration/verified-checkpoint-image35-1.json), built only from committed `ced7c9d4`, matches all 560 Rust-tree inputs from the 351-test strict workspace. All eleven packaged gates pass: 1,024 captured API checks, proxy/auth/hash/settings profiles, stored guides, playback preflight, fresh installation with actual React bootstrap, category API/React workflows and the formerly failing profile-cache restart. Three actual restart deletions now match Source response, SQL and cache effects while the candidate process remains the same. All eleven owned Docker projects are independently absent. This verifies packaging for the accepted source changes; it does not fix active-media restart behavior, enable default workers/scheduling, close complete milestones or establish new performance gains.


[The typed exhaustion checkpoint](../scripts/rust-job-contracts/verified-epg-exhaustion52-composite-1.json) passes strict workspace Clippy and 351 default tests (361 external-fixture tests intentionally excluded there) on 561 captured Rust-tree inputs. Four actual Original/native busy-exhaustion pairs, including known caught status P0001 faults, pass the 52-check gate plus the 80-check refresh regression; constructor19, ETA20 and duplicate24 also pass. Per-run typed reasons become normal null only after physical reporting, terminal cleanup and Released settle. Concurrent reason isolation, held reporting/release, canceled observers and operational controls are separately observed; an aborted caller's return value remains unobserved. Old FileJob APIs retain their Failed outcome. Terminal SELECT failures, other SQL/capacity/reset/uncertain/coarse failures and default application intake remain separate gates; a PENDING control is not Original parity.


[The generated contract work register](rust-contract-register.md) now preserves all 592 resolved route entries, 798 operation work items (including 322 unresolved-method placeholders), 362 separate OpenAPI operations and 250 static non-HTTP candidates. It supplies stable IDs and explicit role/fixture/assertion/result mapping gaps, with initial scoped evidence links. Module-associated tests and discovery do not mark entire routes passed; row-level case mapping remains incomplete. This improves M0 tracking without changing any milestone's acceptance status.


[Read-only runtime method inspection](rust-contract-register.md#runtime-handler-candidates) adds handler candidates for 166 of the 322 method-unresolved route entries, preserving all 592 original resolver identities and their exact order. Inspection executed no views or SQL; the owned setup used its normal readiness request and was removed. Default permission attributes and dynamic policy references remain candidate evidence only, not authorization or parity acceptance. M0's explicit case/role mapping work remains open.


[The guarded EPG exhaustion checkpoint](../scripts/rust-job-contracts/verified-epg-guarded-server-composite-1.json) passes 353 default workspace tests, strict all-target Clippy, 131 guarded checks plus 80 targeted refresh regressions, and constructor/ETA/duplicate replays (19/20/24 checks). All four copied-database runs bind the same 561 Rust inputs and pinned Original archive; default databases are unchanged and owned Compose resources independently absent. Normal null mapping is limited to known file/parser exhaustion after physically settled guarded PostgreSQL ERROR and release. UPDATE0, NotOwner, general workers/schedulers, HTTP202, packaged-image acceptance and performance remain separate work. M6 remains in progress.


[The eager Original scheduler capture](../scripts/rust-job-contracts/verified-original-database-scheduler-startup-1.json) passes 16 startup/default-installation and changed-sentinel reload checks in an owned copied database. It records stale cached entries after unsignalled bulk updates and reload after ordinary saves/deletes or explicit marker updates. No native scheduler, task execution, process restart or performance acceptance follows; M6 remains in progress.


[The bounded interval decision checkpoint](../scripts/rust-job-contracts/verified-interval-scheduler-component-1.json) matches 25 actual Original controlled-clock decisions and requested mutation effects. Its immutable 564-input workspace passes strict all-target Clippy and 355 default tests (361 external-fixture tests ignored). Four initial HTTP listener EPERM failures are retained; the unchanged frozen runtime passes with local-socket access. Native SQL persistence, full scheduler startup/publication/crash behavior, packaging and performance remain open. M6 remains in progress.


[The settled return DTO checkpoint](../scripts/rust-job-contracts/verified-epg-return-dto-composite-1.json) passes 356 default workspace tests, strict all-target Clippy and four actual copied-database replays on exactly 565 Rust inputs. Six Original/native race pairs pass 93 checks alongside 131 guarded and 80 refresh checks; constructor/ETA/duplicate controls pass 19/20/24. UPDATE0 and acknowledged NotOwner remain explicit outcomes; legacy NotOwner errors are preserved. Native preservation of a replacement lock differs from Original unconditional deletion and is documented, not claimed as internal effect parity. Default workers, application HTTP202, packaging and performance remain open. M6 remains in progress.


[The selected main integration checkpoint](../scripts/rust-integration/verified-main-integration-components-1.json) binds exactly 600 Rust inputs: strict workspace Clippy and 362 default tests pass (461 external-fixture tests ignored), alongside six actual component replays for VOD50, refresh ETA60, Series155, refresh HTTP127, index Original20 plus nativeHTTP1, and unified188 exact HTTP comparisons. One browsable HTML template remains excluded; earlier four exclusions were corrected to three JSON406 cases and one HTML200, and the three JSON cases now pass. Experimental EPG jobs/recovery default OFF; configured recording rules, unsupported raw IDs, remaining recovery branches, packaged browser/workflow/restart gates and new performance remain open. The current accepted packaged checkpoint remains image35 until a new image passes its gates. Original authors retain all application/algorithm credit.


[Image36 packaged acceptance](../scripts/rust-integration/verified-checkpoint-image36-1.json) verifies all 600 compiled Rust inputs against committed `bce2b106`, strict workspace Clippy and 362 default tests (461 external-fixture tests excluded from that default run). Eleven packaged regression groups pass with owned Docker resources independently absent. Additional [unified React](../scripts/rust-vod-api-contracts/verified-unified-image36-1.json) and [complete refresh workflow](../scripts/rust-job-contracts/verified-complete-refresh-workflow-image36-5.json) proofs remain scoped: the latter performs two imports, six native task results, exact guide/index/event projections and UID65532 path checks. Experimental EPG and VOD recovery flags remain OFF. Actual capped-user207 and immediate-reopen208 recovery failures are retained; isolated fast-entry212 and HTML prototypes are not included in this image. No new performance or complete milestone acceptance is claimed.


[The immediate VOD reopen source fix](../scripts/rust-integration/verified-vod-fast-entry-source-1.json) passes strict whole-workspace Clippy, 362 default tests and 52 actual recovery fixtures on the same 600 Rust inputs. A fresh retained classification removes reliance on the periodic loss monitor; settled no-write admission failures avoid decrementing unrelated bodies. The actual unchanged-runtime negative is retained. A new packaged image must repeat actual SQL/auth/Redis restart208 before packaged acceptance; capped-user/public-stop and pre-activation uncertain-entry receipt behavior remain separate gates. Recovery stays OFF by default; no performance or complete milestone acceptance follows.


[Image37](../scripts/rust-integration/verified-checkpoint-image37-1.json) passes the eleven baseline packaged groups and two additional unified-React/complete-import groups. All 600 compiled Rust inputs equal the strict workspace/default362/actual52 fixture source. One clean-install fixture attempt raced PostgreSQL temporary initialization-server shutdown; a TCP-only readiness check passes the unchanged image and the failure is retained. Actual recovery216 remains failed: first immediate reopen503, subsequent reopen200, old body continues827,392 bytes. This is not packaged recovery acceptance. The separate image36 unified catalogue functional comparison passes exact 9,000-entry/24-result responses with one/ten simultaneous readers; its resource matrix requires new measurements and does not inherit earlier gains.


[The image36 unified Both/All metadata experiment](rust-unified-performance.md) passes all six predeclared pairs at one request/second per reader. Median paired CPU decreases 13.6%/13.5%, sampled peak group working memory decreases 35.0%/34.4%, and completion p95 decreases 17.0%/16.4% for one/ten API readers, with throughput 1.000. All 3,960 measured responses match Original bytes; 12 phases/720 samples pass zero error/late/OOM/reset gates. Public numerical evidence retains all 20 qualification/measurement phases (840 samples/4,258 traces). Parent independently recomputes raw CPU, memory sums, limits and paired medians, and verifies owned cleanup. The initial controller qualification-only failure remains recorded. These gains apply only to exact image36 unified-list requests; full milestones and later-image performance remain open.


[The shared UI permission and captured HTML component](../scripts/rust-integration/verified-ui-auth-html-components-1.json) passes strict whole-workspace Clippy and 365 default tests (465 external-fixture tests excluded there) on 622 frozen inputs, retaining the committed immediate-reopen code. Two real copied-database fixtures pass 12 ignored tests: 297 UI permission comparisons, 99 additional categories, six nonempty event reads, three pre-view HTML fault controls, 56 HTML observations, 188 unified and 393 ordinary JSON comparisons, plus retained physical resource controls. Only one validated random CSRF mask slot differs in HTML; deterministic bytes remain exact. Both owned projects are independently removed. Assets, broader HTML contexts, packaging and full M5 remain open; these source changes inherit no historical performance claim.


[The integer/query and optional session lookup source checkpoint](../scripts/rust-integration/verified-json-lookup-components-1.json) binds 628 Rust inputs: strict whole-workspace Clippy, 367 default tests (472 external-fixture tests excluded there), and 71 actual fixture tests pass on the identical source. The combined real fixtures match 77 integer/query edge responses alongside the auth, HTML, unified and ordinary regressions; 58 VOD/lookup controls include lost replies, four-second physical ownership, cancellation, capacity and shutdown. Optional read errors now follow the captured Original fallback without retrying mutations. Query conversion errors preserve parse/logger/SQL/count/link order. Public-stop policy227/231 is excluded; actual packaged216 recovery, header-stripped identity, assets and broader M4/M5/M6 remain open. No previous performance gain transfers to this source.


[The actual Original shared-buffer reference](../scripts/rust-core-contracts/verified-original-buffer-source-1.json) records 12 real-Redis observations for split/truncated TS, flush/stamping/reset, a two-second expiry, cursor positioning and producer stop. It preserves full synthetic bytes and audits the untouched default database. The effective configured default chunk size comes from BaseConfig (188 × 1361), not the larger constructor fallback. The prospective last-consumed-to-next-read mapping is explicitly derived, not a native observation. Native comparison and the faithful paired shared-buffer benchmark remain required for M1; no helper-only or Rust-only fanout result is promoted.

[The later M1 mark-boundary component](../scripts/rust-core-contracts/verified-core-mark-components-1.json) fixes pending-PID discontinuity stamping against a13th real-Redis Original capture and an exact-byte native HTTP switch replay. Its630-input workspace passes strict Clippy and369 default tests (472 fixture-dependent tests remain ignored). The full native shared-buffer trace/resource matrix, held-reader accounting and paired core benchmark remain open; image38 predates this component.

[The combined scheduler/recovery source checkpoint](../scripts/rust-integration/verified-scheduler-recovery-components-1.json) retains public identity per generation while old bodies keep their actual ownership; public stop is consumed on Source8192-byte boundaries and final completion keeps the body owner until acknowledgment. Admin stop has independent bounded control admission and retains service/credit through actual replies and caller cancellation, without replaying uncertain writes. Known single-interval timers reach native Refresh/Series0 workers and OriginalAsyncResult, with physical SQL/Redis retention, shutdown sync and actual hard-loss evidence. All641 source maps match parent tests and image39. General scheduling, full packaged recovery, performance and milestone closure remain open.

[The image41 source/package checkpoint](../scripts/rust-integration/verified-combined-provider-scheduler-recovery-1.json) integrates provider/episode actions into Main using the existing shared reader/gate, known multi-interval scheduling with exact Source load/heap/reload/crash behavior, captured static/Chrome HTML and bounded read-only reconnect classification. No default scheduler intake or broad recovery/performance acceptance is inferred from these proofs.


[The combined Stats checkpoint](../scripts/rust-combined-stats-contracts/verified-combined-stats-parent-1.json) binds 746 Rust inputs, strict whole-workspace checks and 459 default tests (639 fixture tests ignored in that run). Actual normal Main matches 60 Original HTTP responses across JSON/HTML, authentication and live/VOD/catch-up contexts. Four physical controls retain SQL/Redis ownership through cancellation and response bodies, enforce finite admission, and release reservations after completion. [A separate Git-only reconstruction](../scripts/rust-combined-stats-contracts/verified-combined-stats-git-public-1.json) rebuilds all 746 Rust inputs and 42 replay tools and repeats HTTP60/physical4 from public paths. Resource reservations are bounded accounting, not measured RSS gains. The next packaged React Stats gate and new-image performance remain open; M2/M5 are not complete. Original Dispatcharr authors retain all application and algorithm credit.


[The multi-profile notification checkpoint](../scripts/rust-integration/verified-active-multi-current-image48-inputs-1.json) changes only three paths on the combined Stats/VOD 746-input tree. Strict workspace checks and 459 default tests pass; actual multi-profile111, single-profile314 and inactive134 comparison checks pass, with nine physical controls per family. Original unsorted query iteration, per-profile clocks, partial commits and notice/event order remain explicit; owned physical row-order reversal is covered. The opt-in domain supports at most 32 profiles and bounded fetched data. Unknown SQL failures, broader concurrent mutation, default worker enablement, packaging and performance remain open. Historical 713-input component proofs remain distinct from this 746-input union. Original authors retain application and algorithm credit.


[Image48 live React acceptance](../scripts/rust-live-browser/verified-image48-live-ui-1.json) passes normal login, channel preview, advancing decoded video, active Stats with the selected channel, and player close against both Original and the immutable packaged Rust Main. The previously recorded image47 Stats404 and channel UUID405 now return200 in the browser; each backend advances from25 to75 decoded frames. Owned Docker resources are independently absent. React remains unchanged. Broader packaged regressions, realtime transport, new-image performance and full M2/M5 acceptance remain separate gates.


[Image48 retention](../scripts/rust-integration/verified-retention-image48-1.json) passes ten channels sampled through75s and100 late viewers at32 concurrency, retaining the configured4MiB local payload cap per channel. Payload accounting is not RSS or a comparative memory gain. [Its six two-worker lifecycle cases](../scripts/rust-integration/verified-shared-ts-image48-1.json) preserve the real aggregate failure:45 of46 checks pass; Original owner death still leaves one provider slot and input index5 after90s. Native shared demand, global stop and owner-crash cleanup pass, with twelve actual media decodes across both backends. All owned resources are independently removed. [The shipped safe operator CLI](../scripts/rust-operator-contracts/verified-operator-image48-1.json) also passes17 scoped cases; broader Django CLI, help/stderr parity and full M8 remain open. No historical performance result is transferred to image48.


[Image48 packaged acceptance](../scripts/rust-integration/verified-checkpoint-image48-1.json) now binds all746 Rust inputs and1,243 application inputs to committed `fda49b16`. Twenty packaged regression groups pass, including all18 baseline suites/1,024 checks, proxy authentication38, password hashes34, VOD/timeshift decoding and realtime observations, React category/catalogue/HTML/static workflows, provider actions, cached metadata, artwork, complete import and live Stats. The fresh genuine Original import reference and native replay each pass40 checks. React and LICENSE are byte-identical to the pinned Original. Three setup failures, an incorrect category-mode invocation and an expired-guide reference failure are retained with their exact corrections. [Reproduction commands](rust-image48-validation.md) freeze both tools and application inputs. Recovery and the default worker graph stayOFF; new-image performance and full milestones remain open. Original authors retain all application/algorithm credit.


[The strict image48 stale-provider React gate](../scripts/rust-vod-api-contracts/verified-stale-modal-image48-2.json) passes independent exact18-model/sequence resets before each backend, normal login and both providers for Movie/Series modals. Ordered visible results and XC request method/rawURI/header values match; twelve real images decode. All SQL fields/types compare strictly except30 changed datetime paths validated within each own browser interval and four new Episode UUIDv4 paths. Source-only restore/reseed qualification and nine type/clock/identity controls pass. The Source modal calls synchronously; this gate does not infer Celery worker or default graph acceptance. [Reproduction and preserved history](../scripts/rust-vod-api-contracts/STALE-MODAL.md) describe the first selector error, initial positive run and stronger final consumer. All owned projects are independently absent; no runtime/React changes or new performance gains follow from this test gate.


[The first sealed M0 idle/churn run](../scripts/rust-baseline/idle-churn/verified-original-three-repetitions-negative-1.json) is recorded as failed, not baseline acceptance. All102 readers and 18 workload phases pass; repetitions1/3 fail the declared process/FD/socket growth limits, predominantly in common infrastructure. Retained UID/PID aggregates cannot establish a causal leak. Repetition2 loses observer coverage on a permission-denied proc-FD read and supports no later growth inference. All owned projects are independently removed. A separate observer correction/qualification and a fresh unchanged-policy run remain required; M0 and later milestone statuses stay open.


[The denied-proc observer correction](../scripts/rust-baseline/idle-churn/verified-observer-denial-contract-1.json) passes28 controls, actual Source idle/playback/churn functionality and a separate real-kernel control. Seven denied samples remain explicitly incomplete, followed by five stable complete samples after the bounded helper exits; unrelated errors still stop sampling. The kernel helper uses only hash-checked committed support, without private fallback or application privilege changes. The full workload and growth budgets are unchanged. The prior failed three-repetition baseline remains authoritative; a fresh full run and remaining M0 accounting still await execution/review.


[The settled notification-failure source checkpoint](../scripts/rust-job-contracts/notification-terminal/verified-notification-terminal-parent-1.json) binds748 Rust inputs, strict whole-workspace checks and462 default tests (646 fixture-dependent tests ignored in that run). Actual HTTP/result/same-ID87, single-profile317, multi-profile112 and inactive160 checks pass, with physical cancellation/result-backend controls. Only observed effect-stage PostgreSQL ERROR P0001 becomes typed ProgrammingError after rollback and physical close settle; other SQLSTATE/position/read/uncertain failures remain operational. Actual Source AsyncResult/WSGI consumers read the native metadata; no native HTTP route or Main/default graph is added. Same-ID tasks execute while protecting prior successful raw results and aged TTL. Native diagnostics honestly differ from Python tracebacks. Twelve public reconstruction/corruption controls, a fresh Git-only748 rebuild and actual87/five-control replay pass; the rebuilt ELF has its own identity. Image48 remains the accepted746-input packaged checkpoint; no package or performance result transfers to this source-only worker component. Original authors retain all application/framework credit.


[The Source-only file-scanner reference](../scripts/rust-job-contracts/scanner-logo/verified-scanner-logo-parent-1.json) passes102 checks across12 actual Original worker cases for nested/Unicode logo metadata, exact float mtime markers, age thresholds and committed partial failures. First-scan missing/SQL-NULL index selection publishes two real children; same-process rescans preserve them and restart adds two duplicates. All four remain unconsumed until owned teardown. The marker clock is scoped to the scanner dependency; task bodies/events remain Original. The initial missing event subscription failure is retained. This establishes a bounded Source contract for the next Rust component; no Native, child execution, image decoding, default graph, performance or full-family acceptance is claimed.


[The corrected complete Original idle/churn reference](../scripts/rust-baseline/idle-churn/verified-original-three-repetitions-corrected-2.json) records valid coverage in all three fixed repetitions and all nine stable idle windows. All102 readers/18 workload phases pass; two actual denied samples stay incomplete and sampling continues. Source process/FD/socket growth limits remain failed, with most growth in common infrastructure. Pinned warm database pooling supplies possible context, not causal proof. The original invalid run and all unchanged-budget negatives are retained. This completes observation of that Source scenario; remaining M0 workloads/accounting and all Native performance comparisons are separate.


[The completed image48 live resource experiment](../scripts/rust-live-perf/image48/README.md) retains all18 measured phases,1,062 in-window samples and666 authenticated readers. All nine fixed pairs pass unchanged budgets. Independent raw-counter, capture/decoder and source/support hash reviews pass;77 publication/control tests pass without skips, with the separate public wrapper running30 corruption controls. Exact source revision `fda49b16`, immutable image digest and measured support revision `28122faf` are recorded. Background workers, full application resource accounting and remaining milestone gates stay open; no earlier checkpoint observations are relabelled.


[The actual Original scanner timestamp extension](../scripts/rust-job-contracts/scanner-logo/mtime/README.md) captures43 cases and509 checks across finite epoch/nanosecond values, exact raw Redis marker bytes/TTL, SQL/events and normal-null worker results. All executed tool/raw report hashes and unchanged default database were independently reviewed; fresh owned resource queries are empty. Requested nanoseconds, actual stat float and formatting candidates remain distinct. This is a Source-only contract, not ordinary-timestamp native scanner or task/application acceptance.


[The actual Original createsuperuser CLI reference](../scripts/rust-operator-contracts/createsuperuser/README.md) passes12 subprocess cases/55 checks against an owned copied database, including environment inputs, Unicode normalization, duplicate/error paths and the observed missing-versus-empty-password distinction. All24 stream hashes and unchanged default database are reviewed; fresh owned cleanup is empty. The initial wrong empty-password oracle is retained. Native CLI implementation, interactive forms, packaged login and full M8 remain open.


[Four Source playback accounting attachments](../scripts/rust-playback-baseline/verified-source-accounting-1.json) now qualify mixed readers, stall, failover and warmed ten-channel before/active/local-close/retention states. Existing workload/sample controls and32 portable checks pass; all final clients/upstreams clear. Unlimited provider capacity and unset service caps remain explicit; Source failover warning, oracle negatives and pre-start cue/output failure remain preserved. This is Source functional accounting, not Native resource gains or full M0 closure.


[The bounded native createsuperuser component](../scripts/rust-operator-contracts/createsuperuser-native/README.md) passes actual Original/native22 cases/95 checks per side and154 strict typed comparisons, with only ten individually bounded creation clocks. Its750-input source passes strict whole-workspace/all-target Clippy,464 defaults/648 ignored fixtures,12 portable corruption controls and two actual SQL ownership controls. Static peer review found the pre-fix SQLx ERROR-before-ready gap; explicit acquired-connection Sync/close and a held-ReadyForQuery uniqueness-error control now cover it. Full CLI byte parity, interactive/remaining Unicode grammar, packaged execution and M8 stay open; original authors retain full credit.


[The pending VOD follower source checkpoint](../scripts/rust-vod-contracts/pending-follower/README.md) admits an owned follower during acknowledged post-claim provider reservation, matching the captured early Source TCP success/busy schedules. Parent review removed an unbound Lua argument exception and retained provider>0 validation with an actual zero-provider rejection control. The combined751-input CLI/follower source passes strict whole-workspace checks,464 default tests/655 ignored fixtures and54 real Redis/HTTP/media gates. Old VOD worktree experiments remain preserved; isolated Source tooling does not overwrite historical Factory flags. Real Main SQL/auth packaging, Source late-body/counter boundaries, default enablement and full M4/performance acceptance remain open.


[The new Source-only capacity/switch/sustained references](rust-m0-baseline-progress.md) verify actual finite1/10 provider boundaries, shared reuse/overflow refusal/release, real admin POST switching and one continuous15/60s read followed by explicit close/70s retention. All final clients/origins/slots settle to zero;22 portable guards pass. The relocated sustained harness leaves measured image47/image48 namespaces untouched and collects zero resource samples. These close named Source functional evidence gaps, not Native/resource/fullM0 acceptance.


[The bounded worker-inclusive Original workflow](rust-m0-concurrent-bounded.md) passes real2 imports/6 Django-Celery tasks/44 durable checks while one authenticated viewer remains open. Normal journal16 records/5,887 bytes and all process pipes settle;14 portable guards and separate genuine Django overflow controls pass. All four pre-work census parts are complete/zero-denial; the inherited denial-merger omission still blocks resource acceptance. No during-workload sampler or Native gain is claimed; M0 remains open.

[The combined bounded scanner integration](../scripts/rust-job-contracts/scanner-logo/task/README.md) is committed as `6054f7b5`, preserving the CLI and corrected pending-follower source. Fresh strict Clippy, 476 default tests (667 ignored), 85 task checks/five physical controls, 5,135 ordinary-effect checks/four physical controls, legacy/notification/expiry replays and 54 Redis/HTTP playback fixture regressions pass against its complete 762-input map. Separate actual Original timestamp70/task24 references are published. The scanner remains explicitly constructed, logo-only and unregistered in Main; helper descendants, nonempty M3U/EPG and broader traversal remain open. Original authors retain all application/algorithm credit.

Image49 is a candidate, not an accepted checkpoint. Its separate three-case packaged Main delayed-provider gate passed abort but returned503 in success and busy cases, where the Original fixture returned200/429. Early follower playback, stop preservation, actual hold bounds and owned cleanup passed. The Original fixture held a post-INCR method return while the native fixture held a physical Redis reply; [Independent physical socket-delay authority](rust-vod-pending-main-image49-failure.md) now confirms Original success/busy after2.4s and the native two-second observer error before actual successful settlement. A narrow retained-result correction is being tested. No failed gate, component pass or image48 performance result is transferred into image49 acceptance. At that failed observation, image48 was the accepted checkpoint; the later image50 scoped checkpoint is documented above. M0–M8 remain in progress and M9 remains planned.

[Image49 regression qualification](rust-image49-validation.md) now records twenty successful packaged groups, a fresh Original import reference and seventeen shipped safe CLI cases. All1,248 application inputs match committed `dffe0920`, React/LICENSE match Original, and all22 owned projects are independently empty. The separate delayed-provider Main failure above still blocks candidate acceptance. No image49 performance or full milestone completion is claimed.

[Packaged bounded createsuperuser](../scripts/rust-operator-contracts/createsuperuser-packaged/README.md) now passes22 cases/95 checks and154 typed Original comparisons using the actual image49 command as inspected UID65532 with read-only root/capdropALL. Default database and all44 transcripts are independently checked; owned resources are absent. Only ten bounded creation clocks vary. This closes packaged execution for the bounded CLI grammar, while full CLI/M8, image49 application acceptance and performance remain open.

[The portable literal M0 crosswalk](../scripts/rust-baseline/crosswalk/README.md) links12 observed HTTP method/role cases, three task candidates and eight baseline scenario/budget entries to unchanged public authorities. Nineteen controls and validation in a Git-public-only tree pass. It preserves unresolved methods, null undeclared ceilings and valid Source growth-budget failures. This is static summary/accounting validation; it runs no workloads, completes no endpoint and does not close M0.

[Resource observer integration qualification](../scripts/rust-baseline/concurrent-resource/README.md) now preserves the first setup failure and the corrected ten-second four-role Source pass: ten resource rows, nine CPU intervals and five complete censuses. An actual helper timeout verified all four container PIDs were zero before host-client teardown, with both pipes drained and independent cleanup empty. The default database audit passed before deliberate teardown. Root independently passes84 controller and five public validation controls. This qualifies instrumentation; full concurrent-workload resource repetitions, growth acceptance, Native comparisons and M0 remain open.

[The scoped retained-result correction](rust-vod-retained-result.md) preserves Coordinator’s global two-second timeout and the outer three-second observer, while returning the actual physical reservation result to the supervised VOD transition. The combined766-input source passes strict whole-workspace checks,476 defaults/682 ignored and77 physical/component playback controls. [The explicit first-scan Index helper](../scripts/rust-job-contracts/scanner-logo/index-helper/README.md) passes80 checks/four physical controls with actual Source-produced parent messages, real Index consumption and partial-publication metadata; prior scanner and legacy-worker regressions pass on the same new binaries. Before-ACK admission remains an explicit conservative boundary. Main/default activation, large traversal, M3U/EPG signals, packaged image50 and full M4/M6 remain open.

[The actual Original scanner M3U/EPG branch reference](../scripts/rust-job-contracts/scanner-logo/file-branches/README.md) captures17 cases/217 checks, preserving29 real child frames until owned teardown. It records inactive M3U’s group child/default-profile/disabled Beat effects, both EPG refresh publications, first-index ordering and the distinct caught EPG versus task-failing M3U partial-commit paths. The default database remains unchanged. This is Source-only authority for the remaining scanner implementation, not native/Main/default-graph or performance acceptance.

The image50 packaged Main pending-follower gate now passes all three cases/six Source/native phases with physical Redis replies held2.4s on both backends, genuine Original client60s timeout, real SQL/Bearer/early8192-byte playback, typed busy429 and private other-movie reservation protection. All three owned projects are independently empty. Literal post-close metadata differences and uncaptured immediate checkpoints remain explicit; this is selected early behavior, not full M4, beyond-three-second parity or performance acceptance. The image49 failure is retained; image50 full package regression review remains separate.

[Image50 acceptance](rust-image50-validation.md) now includes all twenty fresh packaged groups plus Original import prerequisite, two shipped CLI gates, candidate-only shared-worker lifecycle,100 retained-history readers and the physical Source/Main three-case gate. All1,263 committed application inputs and29 owned-project inventories are checked. The failed first retention invocation is preserved with its old16MiB expectation; the existing4MiB contract passes explicitly. This is scoped packaging/functional acceptance, with full M4/M6/worker graph/soak and new performance still open.

[The fixed three-run Original resource reference](../scripts/rust-baseline/concurrent-resource/reference/README.md) now has valid coverage in all three runs,18 task executions/132 repeated durable-check executions,774 resource observations and335 eligible of342 censuses. All workload/decoder/default-DB/physical-cleanup checks pass. Each run retains the same Source growth-budget failures: process1/limit0, FD144/16, socket18/16; other declared budgets and45 repeated interval checks pass. Thirteen portable corruption/reconstruction controls recompute the published numerical projection. These are accepted bounded baseline observations, not Source budget passes, Native gains, causal leaks or complete M0; the later outage and metric attachment are indexed in the [M0 baseline](rust-m0-baseline-progress.md).

[The779-input scanner parent worker](rust-scanner-mapped-parent.md) now passes Root rebuilt621 assertions/17 Source cases/eight physical controls, strict workspace checks and560 defaults. It preserves known model/schedule/marker/event/child-publication behavior in its declared flat-file domain; children remain unconsumed. Main/default graph, child execution, broader domains and full M6 remain open. Image51 is unchanged.

The [scanner-owned M3U import component](rust-scanner-m3u-consumption.md) now passes Root275/8 interoperability,16 held-SQL and66 held-publication checks on one rebuilt788-input component, plus strict host checks and566 defaults. Six domains execute natively; missing-file/group-SQL refuse before ACK and fall back to Original. Full audit history currently refuses; the separate Original323/15/45-task audit reference supports the next gate. Main/default consumers remain unchanged and M6 remains in progress.

The next bounded Source-only cohort adds26 HTTP assignments from167 unique successful named Original3362 test workflows (177 operation/workflow associations), through206 static request sites. There are129 direct sites and77 helper-associated sites; these are source coordinates, not a live request-count or TCP transcript. The exact Original tests, two URL files, setup/teardown, helpers, role creation/level writes, decorators, mocks, original assertions and successful result blocks are public and Source-hash bound. No new Source run was performed merely for labels/counts. Five newly assigned APIView method placeholders remain null, and all322 unresolved declarations across the register remain unresolved.

Current Source-assignment totals are55 HTTP setup/assertion/result records (44 partialOriginal framework-test assignments and11 partialcaptured-case assignments), plus the unchanged seven nonHTTP assignments (one model and six tasks). All250 nonHTTP records are exactly preserved from916. There remain743 HTTP role matrices unmapped and243 nonHTTP static candidates; Native fullyverified remains0. The old29 HTTP operation records and all772 operations outside the new26 remain byte-semantically unchanged.

This cohort does not assign queryset/model/resolver-only tests to REST, does not infer aliases/HEAD/OPTIONS, and defers unproven SeriesRules/write families. Actual Source levels0/1/5/10 and anonymous context remain explicit;5 is a fixture integer, not an invented UserLevel enum. APIClient and force-authentication limits remain explicit. Formal M0/M1 completeness is not claimed.

The next bounded Source-definition cohort adds13 partial register assignments from55 successful historical Original3362 workflows (63 operation/workflow associations), through90 static request sites:73 APIClient and17 APIRequestFactory. These are source associations, not runtime request counts. Stop/extend's15 workflows are explicitly framework-only direct `as_view` invocations with forced authentication, action and PK joins; they prove no HTTP/router dispatch or TCP. `_make_admin` create-default10 is conditional under `get_or_create`; an arbitrary existing row's effective level is not asserted. EPG's four import tests retain mocked `.delay`, so they prove their original response/mock/query assertions rather than actual Celery execution. Two queryset-only DVR tests and the already-assigned `users/me` PATCH workflow remain excluded.

Current Source setup/assertion/result assignments become68:57 partialOriginal framework-test assignments and11 partialcaptured-case assignments, with730 HTTP role matrices still unmapped. Coverage states are714not-yet-mapped/68partial-executable/16related-needs-mapping. All322 unresolved method declarations remain unresolved, including five newly assigned placeholders. All previous55 HTTP records and all250 nonHTTP records (one model, six task assignments,243static candidates) remain exactly preserved. Every fully-verified flag remainsfalse. APIClient/forced-authentication/mocked-effect limits remain explicit; this does not close endpoint grammar/roles/aliases, Native/browser/performance scope or formal M0/M1.

The [bounded M3U audit-history checkpoint](rust-scanner-m3u-audit.md) now passes independent Root634/15, physical63/3 and capacity25/2 checks plus61 controls on the exact qualified788 map. It ports timestamp-offset/id-delete quirks and settled known audit failures. Empty-hash/stable-settings scope, operator-log/config-order gaps and nonempty-hash admission remain explicit; no Main/default activation, full M6 or new performance acceptance is claimed.

The next bounded Source-definition cohort adds nine HTTP operation assignments and one existing consumer candidate from34 successful historical Original3362 workflows:10 provider APIClient,14 Django Client output and10 synthetic direct consumer-method workflows. There are43 static source sites (33 test-client requests and10 direct consumer calls), not a measured live request count. Provider task/repack/proxy-stop mocks and inherited output network/Redis-cache/log/DB-teardown mocks remain explicit. Exact named reverse declarations/root namespace bind output actions without inventing slash/profile aliases; the two METHOD_UNRESOLVED output rows stay null. Consumer update/receive uses synthetic SimpleNamespace scope.user and AsyncMock send, so it proves no actual WebSocket, ASGI handshake, credential backend, group/broker/Redis transport or lifecycle.

Current Source setup/assertion/result assignments become77 HTTP rows (66 partialOriginal framework and11 partialcaptured assignments), with721 role matrices unmapped. Coverage is705not-yet-mapped/77partial/16related-needs-mapping. All322 method-null declarations remain unchanged. NonHTTP remains250 stable identities: six task, one model and one synthetic consumer partial assignment,242static. All prior68 HTTP assignments and the other249 nonHTTP dictionaries, including all earlier seven assignments, remain exact. Fullyverified stays0. Password-visibility serializer-only factory tests, XC helper-only calls and connect/WebSocket routing utility mocks are deliberately not mapped to HTTP or live consumer transport. No new Source workload or formal M0/M1 acceptance is claimed.

The [fixed normal-capacity VOD observation](rust-vod-normal-admission-observation.md) records Source and Native image51 both returning200 then503, with sampled counters2→1→2 and no dispatch based on outcomes/counters. Actual execution remains exit1/qualificationfalse because its token checker conflated two independent namespaces. Pinned-code-backed offline recomputation separately verifies retained ownership and produces a true scoped scenario result; it does not rewrite the run. Root and peer review found missing mandatory decoder/origin coverage in the first offline packet; the revised packet passes24 controls,12 coverage challenges and27 flag-corruption checks. Both exact69-input restorations, unchanged default audits and empty projects are retained. Two shared-URI origin GET handler intervals overlapped by about8s per backend, with body writes; Range/status were not captured and initial shared viewers read8192 bytes then held. [Literal origin review](../scripts/rust-vod-contracts/normal-admission-observation/origin-review/README.md) therefore makes no single-upstream VOD sharing claim. A separate continuously-draining Range/status characterization is the next gate. Full M4 and new performance acceptance remain open.

This cohort adds 18 partial Source HTTP definitions: ten backup operations, three logs operations, one plugin-logo operation, two VOD-logo actions and two specifically observed slashless variants. It binds 60 named successful historical Original workflows and 87 static source/finite-literal branch sites. These are retained test-client workflows, not fresh TCP observations. Original authors retain all application and algorithm credit.

The current generated register has 95 setup/assertion/result assignments: 84 partial framework-test assignments and 11 captured-case assignments. The other 703 role slices remain unmapped. Coverage is 687 not-yet-mapped, 95 partial executable Source contracts and 16 scoped evidence entries still needing case mapping. All 322 declared-method-null rows remain null, including fourteen newly assigned rows. Fully verified remains zero. All 77 prior HTTP dictionaries and all 250 non-HTTP dictionaries (including eight partial assignments) remain byte-semantically unchanged.

Backup JWT helper/fixture roles and Original mocks are explicit; this offline validation does not generate credentials or perform backups/restores/broker execution. Logs preserve finite URL branches, temporary-file/settings assertions and forced roles. Plugin and image mocks remain mocks. No helper/model-only test establishes a route contract. In particular, three notification factory tests target /api/system/notifications while the runtime routes are /api/core/notifications; they remain excluded pending a correct-route Source fixture. This proposal adds no Native, browser, performance, full-endpoint or formal M0/M1 acceptance. Historical negative observations stay retained.

Preserve all prior dated ledger snapshots. Append this current cohort and its sealed proof rather than rewriting historical counts. No Rust, Main, Cargo, Dockerfile, measured fixture or live/index/ref changes are proposed.

The [bounded configuration-admission checkpoint](rust-scanner-m3u-hash-admission.md) now replaces the inherited conditional hash getter with owned bounded cache/SQL reads, refuses unported hash settings before ACK and masks oversized legacy error TEXT before transfer. Root176/14 admission,32/2 physical and66/5 publication cases pass on one independently rebuilt788-input component, with36 controls; same-source strict/default566 checks pass. Source70/25 and the actual null-worker13/1 reference are separate authorities. Main/default consumers remain off, current packaging is a separate next gate and M6/performance acceptance remains open.

The [continuous same-session VOD characterization](rust-vod-sharing-characterization.md) now passes one fixed Source/image51 pair: both200 requests drain continuously with matching byte progress, four actual1MiB prefixes decode, and both origins record two overlapping full HTTP200 GETs with empty Range/If-Range and no Content-Range. Source resets and Native broken pipes remain distinct partial-write outcomes. The trace supports request accounting, not two-person or single-upstream VOD pooling claims; existing live-TS sharing evidence remains separate. Root50 local controls,25 offline controls and the unchanged57 peer challenges pass after repairing eight portable-validation coverage/time/audit gaps. The first113-file packet and actual raw capture remain preserved; Root independently verifies cleanup/default audits and exact87-input restoration/code/credit bindings. No runtime change, full M4 or new performance claim follows.

The [proxy/plugin and playback Source-definition cohort](../scripts/rust-contracts/proxy-plugin/README.md) adds 19 partial HTTP assignments from 148 retained named Original successes. One proposed timeshift association is excluded because it reaches a different handler. The 258 static helper/request sites are not runtime counts. All previous 95 assignments, all 779 unselected HTTP dictionaries, all 250 nonHTTP candidates, 362 OpenAPI items and 322 null method declarations remain unchanged. Host checks pass 278 of 279 tests with one existing optional skip. The same frozen Python 3.11 SDK selection exited 0; its interrupted text log and recovered daemon exit/destroy receipts are disclosed in the [verification record](../scripts/rust-contracts/verified-proxy-plugin-bindings-1.json). Current totals are 114 partial HTTP assignments and eight partial nonHTTP assignments; no new Source application execution, Native or formal milestone acceptance follows.

The [successful VOD worker-identity component](rust-vod-worker-identity.md) is qualified on an exact 789-input private snapshot: non-test build, strict affected-package Clippy, 80 default executions and 95 real Redis/media controls pass. It ports the Source hostname/PID response header on successful streams, with no added registry packages. Root verifies all selected bytes and an independent Git-only reconstruction. Main/new-image replay, full M4 and performance acceptance for this changed source remain pending; image52 remains the accepted package and its measured gains keep their original binding.

The [bounded literal-URL M3U component](rust-scanner-m3u-url.md) now passes 17 comparison cases/393 checks, 14 Store size-limit cases/100 checks and nine physical recovery phases/182 checks, plus admission and legacy regressions. Its portable 788-input reconstruction and fresh compile are qualified independently; failed fixture attempts remain preserved. This extends the inert library domain without enabling Main/default workers or claiming full M6 or import performance gains. The combined source and packaged image retain separate gates.

The [notification create/repeat component](rust-notification-native.md) now passes the same eight Source-derived requests plus eight Native controls on both its private component and the combined791 snapshot, with independent SQL/response/clock/sequence/default-audit and cleanup review. Administrator creation returns201 and sequential dismissal preserves its sequence. Source forced authentication remains distinct from Native API keys, other CRUD stays guarded501 and full M5 remains open. The additive Source supplement reconstructs all29 original fixture inputs while preserving its original exit1; no Source rerun, operation registration or new performance claim follows.

The [explicit notification Source assignment](../scripts/rust-contracts/notifications-source/MAPPING.md) maps only the observed GET-list, POST-create and POST-dismiss operations. It adds three partial definitions from eight retained framework observations and the closed29-input setup, preserving the original controller exit1, forced-auth limits and recovered cleanup. Current Source totals are117 partial HTTP and eight nonHTTP assignments;681 role matrices remain unmapped. All795 unselected operation dictionaries, all250 nonHTTP candidates,362 OpenAPI items,322 null methods and fullyverifiedfalse remain unchanged. Root21 corruption controls and the300-case host core suite pass (299plus one existing optional skip), and register regeneration is byte-exact. No Source rerun, new Native behavior or formal M0/M1 acceptance follows.

The [combined791 runtime checkpoint](rust-union791-validation.md) is now committed and independently qualified: strict whole-workspace gates and578 default tests, plus fresh combined17/393 import, notification8+8 and95Redis/media fixtures pass.705 default-run ignored cases remain distinct. Public source reconstruction, exact compiler/executable bindings, raw evidence and owned cleanup are retained. Full SDK, new image/Main replay and new performance acceptance remain separate; no formal whole milestone or default-worker activation is added.

The [full combined SDK checkpoint](../scripts/rust-integration/sdk9175/README.md) passes one Docker Compose run on the exact archived `9175d2c5` source: 578 Rust tests, 316 Python tests plus one existing skip, 1,806 byte-exact TS-helper cases and 6,158 unchanged source files. The 705 fixture-dependent Rust tests remain explicitly ignored there. Independent review verifies copied inputs, raw execution/image events and owned cleanup; auxiliary review negatives remain separate. This qualifies the current SDK tooling without accepting Main, application performance or whole milestones.

The [Original notification mutation capture](rust-notification-mutations-source.md) adds 48 actual PUT/PATCH/DELETE framework observations and 1,449 passing checks, with Source/controller exit 0, unchanged named default audits and independent cleanup. The earlier preflight serialization exit1 is retained beside the reporting-only correction. Portable validation, 17 corruption controls and exact 44-input reconstruction pass independent review. This does not change the 117 assigned HTTP Source operations, add Native parity or performance gains, or close M0/M5.

The current selected functional checkpoint is [image53](rust-image53-validation.md): all 28 package groups and scoped Main4s/header/notification gates pass, retaining both failed fixture attempts. Fresh live functional checks cover 222 authenticated readers; Source-only qualification covers 111 readers and 177 in-window samples. Comparative image53 resource gains remain pending; image52 gains remain historical and source-bound. The [three Source48 detail-method assignments](../scripts/rust-contracts/notification-mutation-assignments.md) increase partial HTTP definitions from 117 to 120, preserving all 795 unselected operations and existing non-HTTP definitions. No whole milestone or Native CRUD application acceptance is added.

M0 additionally assigns the [three retained Original schema-cache tests](../scripts/rust-contracts/schema-cache/README.md) to one existing schema-view work item. The register has121 partial HTTP Source assignments; all798 operation identities,322 unresolved method declarations and250 non-HTTP candidates remain unchanged. Mocked schema generation, LocMemCache and direct RequestFactory calls do not establish real schema/TCP or Native behavior.

The [2581d75b paired Group Manager OPTIONS run](rust-m5-2581d75b-group-options.md)
matched five authenticated requests and preserved eight scoped SQL tables.
The [paired XC VOD editor run](rust-m6-2581d75b-xc-vod-editor.md) matched the
unchanged React request and its durable worker effects with Selected workers
and Beat disabled. [The exact Compose candidate](rust-m9-2581d75b-coordinator-coverage.md)
passed 11 of 127 required nodes on one image. The
[six-minute d608 soak prefix](rust-m9-d608-six-minute-soak-prefix.md)
retired all owned roles after ten viewers and a completed refresh, and failed
the idle working-memory limit.

The [stock Django recording route resolver](rust-m0-recording-hls-router-reachability.md)
selected the earlier numeric recording HLS route in all twelve generated cases.
The declaration register retains the later alias and its canonical-path
reachability annotation.

The [stock XC provider deactivation](rust-m6-original-xc-deactivation.md)
verified normal default-Main comparison authority for the one-field React
request, including the schedule change, preserved VOD links, and no new jobs.
The [stock admin restart](rust-m8-original-admin-settings-restart.md)
retained the saved value and the same signed administrator session across an
owned web-generation restart. Both Rust runtime comparisons are pending.

The [native ARM64 guarded restore helper](rust-m10-native-arm64-guarded-helper.md)
built on the pinned Linux ARM64 runner and passed the same-image non-root
runtime, loader closure, notices, and exact container retirement checks.
Full ARM64 application and copied-data release checks remain open.

The [fdede54c paired channel serialization](rust-m5-fdede54c-channel-json-order.md)
matched Stock's top-level order for both ordinary Channel detail responses.
The [default Main XC deactivation pair](rust-m6-fdede54c-xc-deactivation.md)
preserved existing VOD data and matched two notification events with no
new jobs. Both scopes verified exact owned-Compose cleanup.

The [2581 Guide Current gate](rust-m9-2581d75b-current-packaged.md) passed 64
paired wire cases on that image. The [d608 memory attribution replay](rust-m9-d608-six-minute-soak-memory-attribution.md)
passed the unchanged 64 MiB idle check, with 25.7 MiB growth, 20 decoded clips,
and all five roles retired. The prior failed prefix remains part of the record.
The [private 2581 admin restart](rust-m8-2581-private-admin-settings-restart.md)
preserved the saved value and the same signed session across two loopback-only
Main generations. The same-image AIO check retained the default login redirect.
The [pinned Comskip candidate](rust-m10-native-arm64-comskip-candidate.md)
completed a native ARM64 generated-media smoke test at its verified source
revision. Full ARM64 packaging remains open.

The [fdede ordinary exports](rust-m7-fdede54c-ordinary-exports.md) matched
15 M3U requests per backend and the UTC custom guide through XMLTV,
programme Grid, and Xtream short EPG. The
[fdede same-session VOD pair](rust-m4-fdede54c-vod-same-client-reconnect.md)
verified two complete A/V reads, provider-slot release, and natural metadata
retirement. Both scopes verified exact Compose cleanup.
