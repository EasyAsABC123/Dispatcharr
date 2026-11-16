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
Docker build, six package families and the affected Native four-context batch
are pending. VOD repeat5, Source-qualified Series4, manual Comskip3 and loaded
worker/scheduler VLC viewing reuse their retained Source observations.

No positive nonempty-commercial/FFprobe, automatic Comskip, cut, broader metadata/
Episodes/roles/plugins/protocol faults, full milestones or current matched-work
application gains are accepted by source or build validation.
