# Native ARM64 guarded PostgreSQL helper

The isolated GitHub validation branch built and exercised the pinned
PostgreSQL 17.11 restore helper on native Linux ARM64. The runner and Docker
daemon both reported ARM64. The job built with network disabled after
verifying the immutable Rust builder and the three exact Debian package
archives. [Successful ARM64 job](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44859493788/job/136709338463)

The same built image ran the helper as UID and GID 65532 with a read-only
root and no network. It verified the AArch64 ELF, its interpreter,
transitive library search path, six private and three system dependencies,
and five PostgreSQL and Debian notices. The container exited 0 and was
verified absent after removal.
[Redacted receipt](../scripts/rust-integration/m10-native-arm64-guarded-helper.json)

The first native build stopped on an invalid Debian archive metadata
option. The corrected command passed against all three exact packages
before the successful retry. [First ARM64 job](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44859249836/job/136708570164)
[Debian archive command](https://manpages.debian.org/bookworm/dpkg/dpkg-deb.1.en.html)

Full ARM64 application packaging, its copied-data rollback, media donor
and Comskip closure, and distribution remain open. This job exercised
the helper version and loader in a disposable native image.

The original Dispatcharr, PostgreSQL, Debian, Rust, and dependency authors
and contributors retain full credit. [Attribution](../ATTRIBUTION.md)
