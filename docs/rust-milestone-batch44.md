# Candidate44: recording dispatch and basic Series ingestion

Original Dispatcharr, Comskip, FFmpeg and dependency authors retain all original
application, algorithm, configuration and library credit. React is unchanged.

[Candidate43's failed application batch](rust-milestone-batch43.md) remains
unchanged. Comskip now delegates through the existing recording catchall using
the same prepared service, authentication, short SQL scopes, publisher and
shutdown ownership. It adds no conflicting dynamic route. A regression composes
the actual recording/playback/Comskip router and verifies all three closed
services select their own handlers without SQL. The test fixture stages the
exact retained MKV through a current-user-owned transfer file to avoid rootless
UID conversion; an actual65532/cap-dropALL control reads its unchanged hash.

Basic Series ingestion runs inline through the existing refresh-vod root, not
an invented Celery worker. The [retained Source leaf](rust-series-comskip-source1.md)
passes four phases: fresh, repeat, reassignment/global dedup/category replacement,
and empty-Series cleanup. The plain bounded domain preserves NULL years,
category properties, relation flags/timestamps and shared identities. Episodes,
advanced metadata, dates/languages/external identities and broader roles remain
outside the qualified Source scope. The combined Source controller's failed
nonempty-commercial target remains literal; no positive mark/FFprobe claim is
inferred from its no-commercials result.

[SDK61](../scripts/rust-integration/milestone-batch44/checks.json) passes all four
gates with unchanged inputs: 1,148 tests passed, zero failures and 784 ignored
fixture tests across 180 result blocks. There are 1,450 inputs, including
1,444 Rust inputs. Seven source paths change from SDK60. Docker build, six
package families and the one Native four-context batch remain pending.

The Native cohort will run VOD repeat5, Series4, manual Comskip3 and shared VLC
viewing with actual worker/scheduler readiness. Source captures are retained
without replay. Nonempty commercial marks/FFprobe, automatic DVR publication,
cut, broader APIs/faults/plugins, full milestone matrices and current matched-work
application resource acceptance remain open.
