# Guide, recording and stream lifecycle integration candidate

Original Dispatcharr and dependency authors retain full application, media,
algorithm and licensing credit. The React frontend remains unchanged.

This candidate adds the normal SchedulesDirect mapped and single-guide paths,
XMLTV refresh-busy deferred retries, DVR output-profile selection through the
shared live lane, and output-generation checks that prevent a retired reader
from consuming a successor producer's bytes. The TVG reader holds the configured
media directory's physical descriptor so `/app/media` and `/data` can identify
the same directory. Descendant symlinks remain refused. Opt-in input-process
diagnostics report bounded completion counters without URLs or arguments.

The [SDK evidence](../scripts/rust-integration/milestone-batch24/inventory.json)
binds 1,326 inputs, including 1,320 Rust inputs and 55 changed paths. Formatting,
workspace compilation, strict Clippy and workspace tests all exit zero:
1,053 tests pass, zero fail, and 767 are ignored across 176 result blocks.
Ignored tests remain unexecuted. Earlier attempts exposed a SQL error type
ambiguity, an ownership error and the existing canonical UUID regression. These
were corrected; the UUID regression test remains intact and passes.

The committed `df815fbb3d312d191654bf4079f337ecae216430` candidate builds as
`sha256:44ec31e5db05255b2c4643994ddffb7996e3c23451e5e6f60dc212294ebea023`.
All [package checks](../scripts/rust-integration/milestone-batch24/package/)
pass. The1,817 archived files match committed Git:1,320 Rust inputs,495 React
inputs and two credit files. UID65532, production entrypoint, installed assets,
original credit prefix, dependency notices, crypto/PostgreSQL/regex runtimes and
paired fixed-useful-core VLC remux checks pass. The initial build could not find
Docker in PATH; that failed attempt is retained, and build2 uses installed tooling.

The [physical output batch](../scripts/rust-integration/milestone-batch24/physical-output/)
passes ten controls: seven output-store, two profile lifetime/ownership-loss,
and one observed-stop race. The new fMP4 generation control initially lacks its
required fixture; with the actual six-second H264/AAC fixture mounted, it still
ends before returning init bytes. Both failed attempts and exact container,
Compose and Redis-generation cleanup remain recorded. This control is unresolved.
Independent production Main workflows are running; the physical failure blocks
acceptance. Paired resource measurement remains pending. Source23 compact
numbering and its HTTP/model callbacks provide new reference observations;
Native23's normal output-profile workflow passes, but neither observation
accepts this candidate. Remote XMLTV downloads, enabled plugin hooks, broader
Redirect and exceptional lifecycle behavior remain open. No full milestone or
new application performance gain is accepted.
