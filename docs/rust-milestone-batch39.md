# Candidate39: station discovery and aggregate refresh

Original Dispatcharr, Django, Celery, Kombu and dependency authors retain all
application, algorithm, protocol and library credit. React remains unchanged.
This checkpoint integrates the next production worker paths; it does not close
full M6 or other complete milestones.

## Integrated behavior

The normal M3U/XC worker now handles `apps.m3u.tasks.refresh_m3u_accounts` and
the collection refresh POST action using the existing shared queue, worker
slots, Auth pool and shutdown ownership. It selects all active account IDs
without filtering locked/custom/type rows or waiting for child results. Actual
child UUIDs, root/parent lineage, acknowledgement order and backend trails are
retained. An acknowledged publication is distinct from an uncertain attempt.
Unknown or partial work retains its claimed owner instead of reporting success.

Admission checks the complete child domain before claim and checks fresh state
before publication. The existing settings/user-agent/provider predicates gain
read-only observation paths that preserve cached values/version reads while
omitting cache fills. Unsupported child domains guard the entire parent rather
than silently omitting Source-active accounts. The bounded active-account limit
is explicit and does not truncate work.

[Source55](rust-m3u-aggregate-source55.md) exposed the default active custom
account's missing-input behavior. The Single consumer now reuses its real
caught-transfer path for a STD account with no URL/file and absent parsed cache:
no HTTP request, original error status/message, one downloading/error update,
one audit and the Source caught-failure result under task SUCCESS. Existing
locks, task ownership, physical close and finalization remain. Groups-alone and
other unsupported input/cache domains retain explicit guards.

The existing Schedules Direct guide family additionally consumes
`apps.epg.tasks.fetch_schedules_direct_stations`. It uses the same auth/cache,
HTTP transport, SQL/session/event machinery and durable ETA handling. Station
metadata is inserted/updated without programme ingestion. Station tasks use the
Source default null/null limits and do not acquire the guide tasks' locks.
Their Source lookup does not filter source type; mapped/single guide lookups
keep their strict behavior. [Source53](../scripts/rust-integration/channel-stations-source53/README.md)
provides four real API/signal/lineup/error references. Native source creation
and lineup HTTP producers remain unimplemented; worker compatibility alone does
not accept those routes.

## Gates before packaging

[SDK53](../scripts/rust-integration/milestone-batch39/checks.json) records all
four actual gates at exit 0 on unchanged inputs: formatting, workspace check,
strict all-target Clippy and workspace tests. There are 1,135 passed tests,
zero failures and 784 ignored fixture tests across 180 result blocks. The source
map has 1,429 inputs, including 1,423 Rust inputs. Ignored tests do not establish
application or physical acceptance. The preceding SDK52 and fast-lint attempts
remain in local artifacts; the one style diagnostic was corrected without a
lint waiver before this batch.

The [physical schema peer](../scripts/rust-integration/milestone-batch39/schema/README.md)
prepares 15 actual compiled statements (11 matching, three station and aggregate
active selection), checks production bind/result types and rejects four wrong
bind vectors. It is read-only with statement caching disabled and a five-second
statement timeout. Ready and close complete; the default database is unchanged.
Independent cleanup verifies the exact container, project resources and eight
unique original process generations absent. No DML, task or media execution is
claimed by SQL preparation.

## Required application acceptance

Candidate39 builds runtime `bd7984bb207dcbc4c00fe1a69ad432fe5cd6b1f4` as
image `sha256:ad1242dd0f60c60d4425c29bd549dbbef37a32272956f9b3b277e9b93348f7b2`.
The actual build exits 0 in 136.807 seconds; all
[six package families](../scripts/rust-integration/milestone-batch39/package/inventory.json)
pass with all 55 package CIDs independently absent. The Rust archive matches
SDK53; React and installed original/dependency credits remain unchanged.

[Native56](../scripts/rust-integration/milestone-batch39/Native56/qualification.json)
exits 1 in 191.559 seconds with both EOFs and settled process group. Zero-active
refresh passes. The two-account local parent returns SUCCESS with the correct
count and two real child IDs, but child completion times out and its broker
observer also reaches its limit. Later phases are not run. The raw partial
report's `running` top-level status is preserved beside the actual failed whole
execution. Independent cleanup verifies three fixture CIDs, three original
positive host PIDs and empty project inventories.

The causal code review finds that the local consumer still requires the
scanner parent's result to be null. Actual aggregate parent results are strings.
A private correction gives only an authenticated, exact broker-head/digest scope
the appropriate parent-independent admission; legacy scanner receipt checks,
SQL/file admission and exact CAS stay intact. This run does not qualify that fix.

[Native57](../scripts/rust-integration/milestone-batch39/Native57/qualification.json)
passes all four station worker cases at actual exit 0 in 88.384 seconds, with
both EOFs, settled process group and independent three-CID/three-positive-PID
project cleanup. The workload uses the real pinned Source Celery publisher in
the Native fixture, so Native HTTP creation/lineup routes are not qualified.
Three newly produced countdown-3 ETAs show actual early unacked reservations,
idle/empty SQL state and no terminal backend before their actual due time,
followed by real successful execution. Lineup addition uses an immediate Source
workload frame. No historical ETA was replayed or shifted. Actual station rows,
progress/channel deliveries, partial provider failure and 4102 idle results match
the retained Source53 contracts.

Required guards, nonempty cache/update cases, wider metadata and provider errors,
series/VOD ingestion, model-enabled matching, concurrency/loss/rollback and full
milestone performance remain open. Candidate38's application and historical
performance results retain their exact source/workload scope. No new viewing
CPU or memory gain is claimed at this checkpoint.
