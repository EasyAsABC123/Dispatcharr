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
A committed-source Docker build, package checks, the complete affected application
cohort and a fresh complete paired performance study remain pending. No performance gain or full milestone is
accepted. Candidate45 measurements and failures remain immutable.

The existing public recording-Connect launcher also gains a selectable
`catalogue-series-comskip-vlc` workflow. It exports25 byte-exact selected helpers,
reuses the existing Compose/bootstrap/coordinator, and includes the exact
synthetic Comskip fixture and committed retained Source authorities. Its default
workflow stays recording-Connect. A clean committed-source execution will run
repeat5, Series4, Comskip3 and shared VLC together against Candidate46; this
will also test the smaller public helper closure. Preparation and private
Candidate45 results do not establish public-runner acceptance.
