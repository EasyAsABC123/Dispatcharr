# Combined-source Compose SDK qualification: aca6d900

All application, algorithm and licensing credit belongs to the Original Dispatcharr
contributors and dependency authors. React is unchanged. These are core SDK receipts,
not full application parity or performance acceptance.

The committed verifier ran once at `aca6d900ad13700ab86b2898134a61e4dba57bc1`,
using its unchanged 1,800-second timeout, a private detached Git context and the exact
archived source. All 8,273 source files were unchanged after execution. The 7,054
verifier-selected hashes and 1,796 actual in-container Docker COPY hashes match.
The Compose file and host verifier are two separate host controls. Runtime networking
was disabled; the Cargo target stayed inside the image. Compiler concurrency was two.

Actual results: **591 Rust tests passed, zero failed, 710 fixture tests ignored**
(184 result blocks); **361 Python tests passed, one existing optional test skipped**
(groups 15/6/322/17/2). The contract subset is 345 tests: 344 passed and one skipped.
TS helper parity passed **1,806 byte-exact cases**, seed 20261109. Standard core/helper
timing artifacts are retained; they establish no new application performance gains.

The actual SDK image is
`sha256:fcc4f0974f919735cfa521a2b292e47362c8f92410cc122d872ca82b4c507c7c`.
The SDK host and Compose commands exited zero. Container exit/destroy events,
settled monitor, exact project/container absence and host PID absence are retained.
No SDK retry occurred. An initial offline packet validator retained the predecessor's
Python total 342; its arithmetic was corrected to 362 without changing the raw run.
That preparation error is separate from the actual successful SDK execution.

`raw-manifest.json` binds 45 exact evidence/input/tool files. The 283,248,640-byte
full Git archive is omitted; its SHA256 is
`f800dd5ee274234109943c3a400fd13201ef499c35af92df73b9a1f49fe1fb9b`.
The Rust794 and coupled5 dictionaries match the qualified combined source map.
The validator binds each map's actual serialization separately.

Offline checks, requiring no Docker, compiler, application or network:

    python3 scripts/rust-integration/sdkaca6/validate.py --repo .
    python3 scripts/rust-integration/sdkaca6/test_packet.py

Reconstruct the exact archived source and private Git context in a fresh directory:

    python3 scripts/rust-integration/sdkaca6/reconstruct.py --repo . --destination /new/private/path

These tools use existing immutable Git objects and do not modify canonical refs/index.
Historical paths in raw receipts are provenance, not dependencies for offline checks.
The source archive can be reconstructed locally; running the SDK/application is a
separate action. The selected functional checkpoint remains image54 until the later
application image passes its own package/Main gates. Image53 performance figures
remain bound to the measured image/workload.
