# Enabled recurring DVR rebuilds and numeric VOD seek

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit. The Docker source manifest preserved all 497
React, license and attribution inputs.

The exact build from `dd41febfc2120f776734d2ab85973899622f3816` passed. The
private Rust archive had 1,485 unchanged regular file contents after
timestamp normalization. Binary check, formatting and strict Clippy passed.
The focused checks recorded 916 passing Rust tests and 214 ignored
external-fixture tests. The [validation plan](rust-development-and-validation-plan.md)
records why the first cached local results were superseded.

Both packaged backends passed the enabled DVR purge, edit and activate
checks. Each rebuild created two non-UTC overnight recordings beyond 14
days, two clocked Beat tasks and two artwork jobs. The activation case
matched the channel-name fallback. Existing future rows were purged and
the scoped fixture verified exact retirement of both projects. Coverage
for concurrent maintenance, larger calendars and other operators remains
open.

The numeric VOD seek fixture exited 1 during its first Original guest
call. It recorded zero HTTP observations and removed both owned roles.
A fresh replay is necessary to qualify the product's numeric seek path.
These functional checks add no performance measurements.

[Exact Compose and Rust evidence](../scripts/rust-integration/milestone-batch104/functional.json)

## Numeric VOD seek replay

The corrected fixture exercised five actual HTTP requests on each backend
and observed the in-flight session in Redis. All returned HTTP 206 with
the same 65,536-byte media prefix and Content-Range. The provider received
one request per case. Both backends matched offset precedence, seek
precedence, empty-timestamp shadowing, the time fallback and invalid input.
The fixture verified the exact stored offset and complete teardown.

[Numeric seek receipt](../scripts/rust-integration/milestone-batch104/numeric-vod-seek.json)
