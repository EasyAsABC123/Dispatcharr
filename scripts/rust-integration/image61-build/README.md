# Image61 immutable build evidence

The Docker build completed with exit 0 from committed revision
`e96aac33399600df347c799e8ac5135d49f10d85`. Its image is
`sha256:9e463647fd7cb822b3b96822408b203f54029d7de7f2546d97f4ff1d83e6004c`.
The normalized source archive contains 873 Rust inputs, 495 unchanged Original
React inputs and LICENSE/ATTRIBUTION.md: 1,370 application inputs in total.

The disposable, read-only Docker probe exited 0 as UID 65532. Its 114 installed
React/credit file hashes equal the retained image57 inventory. Both fresh exact
container queries were empty after its automatic removal. The first probe
invocation failed before Docker execution because the tooling environment was
not loaded; that failure is retained separately. The unchanged probe succeeded
after loading the repository tooling environment.

Verify the retained source hashes against immutable Git objects, reconstruct
the exact normalized archive and restore its application and two builder inputs:

```sh
python3 scripts/rust-integration/image61-build/verify.py --repo . --output /tmp/image61-inputs-fresh
```

The output directory must not exist. This command does not execute Docker,
Cargo or the application. The packet retains the literal build manifest,
runtime package inventory and probe receipts, including original host identities
and command arguments. Verification checks those recorded observations; it does
not rerun the historical physical probe or independently rebuild the image.
The private build log and source archive are not duplicated here; the exact
archive is reconstructed from Git and checked against its retained hash.

The scoped DVR library is included in this image, with its existing opt-in and
process-exit limitations. Default DVR workers remain off. No new Main workflow,
SDK/package matrix, media parity, recovery or performance acceptance is granted.
The measured image57 live-stream gains retain their original workload and source
binding. All Original application, React and algorithm credit remains with the
original authors; reconstruction preserves LICENSE and ATTRIBUTION.md.
