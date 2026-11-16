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

At this source checkpoint the committed Candidate39 image build and all six
package checks remain pending. Two complete affected cohorts follow: four
aggregate HTTP/root/child/status/cache/audit/event phases against retained
Source55, and four station worker/ETA phases against retained Source53. The
station workload uses the real pinned Source Celery publisher in the Native
fixture; it must not be described as a Native HTTP producer. Newly produced
future ETAs require an actual early unacked reservation with no SQL/backend
result, followed by due completion; replaying an old ETA cannot prove this.

Required guards, nonempty cache/update cases, wider metadata and provider errors,
series/VOD ingestion, model-enabled matching, concurrency/loss/rollback and full
milestone performance remain open. Candidate38's application and historical
performance results retain their exact source/workload scope. No new viewing
CPU or memory gain is claimed at this checkpoint.
