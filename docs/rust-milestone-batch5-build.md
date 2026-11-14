# Metadata/database Docker candidate

Original Dispatcharr and dependency authors retain all application, algorithm
and license credit. Celery/Kombu notices are included in the runtime.

Committed runtime `95da37d9f0bd6dcca5f678d114bdac994de9b8de` builds as image
`sha256:b7c1ca84201f155dfe7a4b285cff97b6e66522b4d89681830fa63e10eea11c2a`.
The source tree is `6570edaa7fcc5d21c266d229e279d2bf7dfc6da8adb097a3ff2ab30ce1725a83`;
the production ELF is `4126e8bf3f16ce9f512f7f98841e9b614502c4ace09ebd93e216d6c17e23dd56`.

[Actual build and physical probes](../scripts/rust-integration/milestone-batch5-build/Root-build-review.json)
pass. All1,427 archived files match Git:930 Rust-directory files,495 React files
and both Original license/credit files. The Rust closure matches the
[896-test candidate](rust-milestone-batch5.md). All114 original installed React/
credit hashes are unchanged; two additional exact upstream library notices are
installed. UID65532, the real ELF entrypoint and `/app` working directory pass.
GPG2.4.8 and27 crypto/trust/owning-license hashes are independently verified.
Exact probe-container absence is recorded.

The first build exited1 before compilation because the daemon was absent after
a host reboot. The failure remains captured. The task-owned rootless daemon was
restored with its existing data/images under a transient user service; the same
Git source then built successfully. This infrastructure repair does not change
application acceptance or establish cross-boot performance results.

Runtime Core99, affected jobs, React and PostgreSQL TLS/UDS cohorts are separate
required gates. Build success does not pass them or a full migration milestone.
