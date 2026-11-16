# Candidate42: isolated commercial-detection libraries

Original Dispatcharr, Comskip, FFmpeg and dependency authors retain all original
application, algorithm, configuration and library credit. React is unchanged.

This successor preserves Candidate41's VOD update and manual Comskip Rust code.
Only the Python media collector changes: Comskip gets its own executable,
loader and shared-library directory under `/opt/dispatcharr-comskip`. The
original image resolves three libraries differently for Comskip and FFmpeg:
`libva.so.2`, `libvpl.so.2` and `libva-drm.so.2`. The original build recipe explicitly separates Comskip's distro-library build
from the local FFmpeg build. The earlier hash guard correctly stopped
[Candidate41's build](rust-milestone-batch41.md). Separate directories
preserve both exact dependency sets. Actual package ownership and existing
byte-exact copyright notices are recorded where available; missing provenance
and the unknown corresponding Comskip source revision remain explicit.

[SDK59](../scripts/rust-integration/milestone-batch42/checks.json) passes all four
gates with unchanged inputs: 1,145 tests passed, zero failed and 784 fixture
tests ignored across 180 result blocks. There are 1,448 inputs, including
1,442 Rust inputs. Only `rust/runtime/collect_comskip.py` differs from SDK58;
compiled Rust and Cargo inputs are identical, so the actual SDK58 physical
[19-statement/six-negative proof](../scripts/rust-integration/milestone-batch41/schema/index.json)
retains its exact SQL/code scope. It does not establish data/task parity.

The actual Docker build passes in 259.641 seconds on source
`09d34a0516c8018f1e92d5a7503a4314434bdad8`, producing image
`sha256:b0d2cf8d0c0677f1afc98ddb6b985bda74cdf532494adfff9b229edd2c307083`.
The [package batch](../scripts/rust-integration/milestone-batch42/package/FAILED.json)
exits 1: archive/assets fails actual Comskip invocation, while crypto,
PostgreSQL, regex, Python compatibility and VLC pass. Its ELF, library,
configuration and notice hashes passed before invocation. Comskip's embedded
RPATH selects the final image's incompatible system libraries despite the
separate prefix. Actual CLI exit is 1 with missing GLIBC/GLIBCXX version errors.
All six actual waits completed and all 55 owned container IDs are absent.
No Native application batch ran on this image.

A bounded diagnostic on this same image preserves both loader controls:
inhibiting RPATH for the named object still exits 1; the empty object-list
argument exits 2 with the original 0.83.001/Donator unsupported-version response.
The binary hash stays unchanged. [Candidate43](rust-milestone-batch43.md) applies
that observed correction. Full migration and matched-work performance remain
open; no application gain is claimed by these package attempts.
