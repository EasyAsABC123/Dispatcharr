# M6 current paired ARM64 XMLTV job lineage

The pinned Original and ordinary Rust application `0c5133b` passed a
fresh XMLTV job replay on Lappy with identical guide bytes. Both used
signed setup and returned 202. Each completed the refresh root and the
ProgrammeIndex and Series children with the expected lineage. The
fixture verified the persisted source and programme, index presence,
and Redis queue and lock drain.

Both arms exited 0 and passed exact Compose cleanup. This was a
functional job replay. The separate synchronized timed test and broader
import and recovery matrix remain open.

Original Dispatcharr, its React frontend, and its dependencies retain
their existing attribution and licenses.

[Machine-readable receipt](../scripts/rust-integration/m6-0c5133b-arm64-natural-xmltv.json)
