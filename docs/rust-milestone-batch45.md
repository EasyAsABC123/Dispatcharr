# Candidate45: compile freshness with normalized source archives

Original Dispatcharr and dependency authors retain all application, algorithm,
configuration and library credit. React is unchanged.

[Candidate44](rust-milestone-batch44.md) exposed a cache correctness failure:
`build.py` normalizes archived timestamps to zero, while Cargo normally uses
source modification times. Its CLI-successful image contains the old
Candidate43 executables. No package/application acceptance or speed gain is
assigned to that attempt.

Every uncached mounted Cargo build now touches all local crate/vendor files,
Cargo inputs and the compile-time static JSON before Cargo runs. Their bytes
stay unchanged. Cargo must rebuild changed local code; registry dependencies
remain cached. Runtime-only Docker layer cache hits still skip the compilation
step. The XMLTV test-stage Cargo command uses the same freshness rule. The
next build must show actual Store/application compilation and a changed Main
ELF, with both executable hashes recorded; operator identity is not required
to change when its linked behavior is unchanged.

[SDK62](../scripts/rust-integration/milestone-batch45/checks.json) passes all four
gates: 1,148 tests, zero failures and 784 ignored fixtures across180 result
blocks. There are1,450 inputs, including1,444 Rust inputs. Only the Dockerfile
changes from SDK61; recording dispatch and basic Series code remain identical.
The committed source `7c75cb07ba08544c903b9db953d00b2309e40f1b` builds
successfully in301.096 seconds, with actual Store/application recompilation.
Image `sha256:09bebef535ebfa571db5950af063b706a613cb559cb7720dd54824384ffd788b`
passes all [six package families](../scripts/rust-integration/milestone-batch45/package/inventory.json).
Its Main ELF changes from the stale43/44 hash to
`1a9988b58013708f0a882b54059904621c479128a3007be46ba0263eb6c05072`;
both executable hashes and actual compilation proof are retained. All55 owned
package container IDs are independently absent. This is correctness evidence,
not a source-equivalent build-speed or application resource comparison.

The [one Native four-context batch](../scripts/rust-integration/milestone-batch45/application/qualification.json)
passes with actual exit0 in230.517 seconds: VOD repeat5 after causal fresh ingest,
Series4, manual Comskip3 and one two-viewer VLC control. Source0/Native4 shares
one bootstrap and resets database/cache/media with fresh Main generations.
Exact retained Source semantics are compared without Source replay. The Comskip
case uses actual exit-one/no-commercials, not a positive commercial mark.
VLC has two authenticated viewers, one input process/upstream/provider slot,
two74-frame clean A/V decodes, actual worker readiness and scheduler ticks,
normal Main exit0 and input retirement. Six exact fixture containers, six
captured original host PIDs, project resources and the UID probe container
are independently absent.

The updated [XMLTV Docker target](../scripts/rust-integration/milestone-batch45/XMLTV/qualification.json)
passes14 tests and all1,144 retained Source32 comparisons with CLI exit0
in14.538 seconds, actual app-stage cache hit and exact owned cleanup. Parser
bytes are unchanged; the parity ELF remains the same. Source workflows are
not replayed and this does not close full XMLTV/API or application resource gates. VOD repeat5, Source-qualified Series4, manual Comskip3 and loaded
worker/scheduler VLC viewing reuse their retained Source observations.

No positive nonempty-commercial/FFprobe, automatic Comskip, cut, broader metadata/
Episodes/roles/plugins/protocol faults, full milestones or current matched-work
application gains are accepted by source or build validation.
