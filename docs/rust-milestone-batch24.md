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

Docker packaging, production Main acceptance, physical generation controls and
paired resource measurement are pending for this candidate. Source23 compact
numbering and its HTTP/model callbacks provide new reference observations;
Native23's normal output-profile workflow passes, but neither observation
accepts this candidate. Remote XMLTV downloads, enabled plugin hooks, broader
Redirect and exceptional lifecycle behavior remain open. No full milestone or
new application performance gain is accepted.
