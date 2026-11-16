# Candidate42: isolated commercial-detection libraries

Original Dispatcharr, Comskip, FFmpeg and dependency authors retain all original
application, algorithm, configuration and library credit. React is unchanged.

This successor preserves Candidate41's VOD update and manual Comskip Rust code.
Only the Python media collector changes: Comskip gets its own executable,
loader and shared-library directory under `/opt/dispatcharr-comskip`. The
original image resolves three libraries differently for Comskip and FFmpeg:
`libva.so.2`, `libvpl.so.2` and `libva-drm.so.2`. The earlier hash guard correctly
stopped [Candidate41's build](rust-milestone-batch41.md). Separate directories
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

The next Docker build and all six package families must pass, including the
existing archive probe's new exact Comskip executable/library/configuration/
notice checks. One subsequent Native cohort will compare five VOD update phases
with the [retained observations](rust-vod-repeat-source1.md), three manual
Comskip cases with the [actual exit-one reference](rust-comskip-manual-source1.md),
and shared VLC playback. Source business workflows are not replayed. Packaging,
Native execution and matched-work performance are pending. All full migration
milestones remain open; nonempty commercial marks/FFprobe, automatic Comskip/cut,
Series/metadata, wider roles/concurrency and historical strict media failures
retain their existing limits.
