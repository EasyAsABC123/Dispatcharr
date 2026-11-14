# Image62 immutable build

Actual Docker build and read-only installed-file probe passed on committed
`ccbdba433df322b89f63521743ca044471318709`. The resulting image is
`sha256:2541ca4ecada8a37cbb7c19b3b666fb825aab5f04b6875a934332e542a6f1059`.
All 1,374 archive inputs match Git: 877 Rust, 495 Original React and two credits.
The probe ran as UID 65532 and verified 114 installed frontend/credit file hashes
against image57. Two fresh exact container queries were empty afterward.

Reconstruct and verify the exact normalized archive and two builder inputs:

```sh
python3 scripts/rust-integration/image62-build/verify.py --repo . --output /tmp/image62-inputs-fresh
```

The output directory must not exist. This command reads explicit Git objects,
checks the recorded archive/package/probe hashes, and executes no Docker,
compiler or application. Build logs are retained privately; historical command
arguments/process identities in the probe receipt are observations, not replay
authority. This is build and installed-file evidence, with no new Main workflow,
package/SDK, full milestone or performance acceptance. The opt-in plugin and
recording libraries retain their documented limits. Original Dispatcharr
authorship, LICENSE and ATTRIBUTION.md remain intact.
