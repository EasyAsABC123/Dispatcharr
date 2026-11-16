# Candidate46: reuse compiled queue observation scripts

Original Dispatcharr and dependency authors retain all application, algorithm,
configuration and library credit. React is unchanged.

[Candidate45](rust-milestone-batch45.md) retains its complete twelve-phase viewing
study, including the one CPU-budget failure. Its observed reductions do not close
resource acceptance. Candidate46 addresses one possible source of repeated work;
its performance effect has not been measured.

Each existing Celery/DVR queue lane now owns one immutable compiled Redis script
for its unchanged queue observation Lua. The transport accepts that shared script
through the existing admission, physical operation ownership, timeout and error
classification path. Queue results are not cached. Polling remains100ms, and
keys, arguments, claims, shutdown and Redis NOSCRIPT behavior remain unchanged.
The existing held-reply test uses the shared script and invokes it again after
physical settlement.

[SDK63](../scripts/rust-integration/milestone-batch46/SDK-summary.json) passes all
four gates: formatting, workspace check, strict all-target Clippy and workspace
tests. Its180 result blocks contain1,148 passes, zero failures and784 ignored
tests. The1,450 hashed inputs include1,444 Rust inputs. Exactly three paths change
from SDK62. Ignored physical tests were not executed by this workspace command.

The [three existing ignored physical Redis tests](../scripts/rust-integration/milestone-batch46/physical/qualification.json) pass when explicitly executed
against one disposable Redis Compose fixture: prepared-script held-reply ownership
and admission plus text reconnect/singleflight/no replay. Actual test exit is zero
in0.295 seconds, with EOF/process-group settlement and exact CID/original host PID
cleanup. Two earlier fixture failures remain literal; neither reached transport
tests. Actual prepared NormalHead lane/NOSCRIPT coverage remains separate.
The committed source `99f0ce781aef613c45a321e44b7cfc5620b621d1` builds
successfully in301.263 seconds with actual output-store, job-transport and Main
recompilation. Image
`sha256:9d6ec63e85b5de402b84f4bd8fe34fbce06c23b1ae04af401d9e4a89360441aa`
passes all [six package families](../scripts/rust-integration/milestone-batch46/package/inventory.json);
all55 exact package container IDs are independently absent. Main ELF
`8087459c7484ae38f8af3c1e49fa4327e4a44fd96c9f42328b0d4b17c90f6433`
differs from Candidate45 and stale43/44. The operator hash is recorded too.
The [clean committed-source public cohort](../scripts/rust-integration/milestone-batch46/application/qualification.json)
passes all four contexts with actual exit0 in231.701 seconds: Movie repeat5,
basic Series4, manual no-commercials Comskip3 and two-viewer shared VLC. Public
tools are committed at `11fded2c465ca78c8f484cb1581fdb9649387b15`; all Rust
inputs remain identical to the image built from99f0ce78. Its26 frozen helpers
and retained Source authorities resolve without the dirty canonical checkout.
One bootstrap and fresh DB/cache/media/Main generations cover Source0/Native4.
Both EOFs and process group settle; seven exact CIDs (six fixtures plus UID),
six captured positive container host PIDs, launcher/controller and owned project
resources are independently absent. This one run qualifies both the affected
application regression and portable export. A new performance study is deferred
until the TLS/advanced-Series combined successor passes functional validation;
Candidate45's failed CPU pair is unchanged. No performance gain or full milestone is
accepted. Candidate45 measurements and failures remain immutable.

The existing public recording-Connect launcher also gains a selectable
`catalogue-series-comskip-vlc` workflow. It exports26 byte-exact selected helpers,
reuses the existing Compose/bootstrap/coordinator, and includes the exact
synthetic Comskip fixture and committed retained Source authorities. Its default
workflow stays recording-Connect. A clean committed-source execution will run
repeat5, Series4, Comskip3 and shared VLC together against Candidate46; this
will also test the smaller public helper closure. Preparation and private
Candidate45 results do not establish public-runner acceptance.

The public launcher's four existing admission/cleanup unit controls pass after
updating their minimal mocked plan with the required Source/Native context
selection. The initial run preserved two KeyErrors before UID execution; this
was a stale unit fixture, with no Docker or application start. Runtime helpers
and Candidate46 executable bytes are unchanged by that test-fixture correction.

Initial clean-export application attempts retain their failures. Attempt1 stops at the
UID probe because owned helper directories inherited mode0700; only directory
modes are corrected to0755, with all input hashes unchanged. Attempt2 exits1
in150.624 seconds with settled EOF/process group: generated receiver setup cannot
import omitted `matching_guest`, so VOD repeat, Series and Comskip stop before
business calls. The independent shared VLC context passes. All seven exact
containers, six captured positive container host PIDs and launcher/controller,
and project resources are independently absent. The corrected26-file public closure includes
the exact frozen guest dependency; the later complete four-context run passes;
these failed attempts do not establish public cohort acceptance.
