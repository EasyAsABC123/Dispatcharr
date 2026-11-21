# Opt-in local native ARM64 package

This command uses the same nine-component assembler and final Dockerfile as the verified ARM64 Compose work. It rebuilds the pinned application revision `9116a8cc` from the committed Git objects and the reviewed text receipts in this directory. Run it on a native Linux ARM64 host with a rootful local Docker engine and BuildKit, Python 3.11 or later, Git, Bash, tar, curl, and dpkg-deb. The repository must contain the pinned source and docs-parent Git objects. Use a local engine reserved for this build so other builds do not share its intermediate image lifecycle.

From the repository with these files committed, run the engine-free source and credit check.

```sh
python3 -B scripts/rust-platform-contracts/native-arm64-combined-assembly/local_build.py --preflight
```

The build requires explicit opt-in and a fresh private output directory.

```sh
python3 -B scripts/rust-platform-contracts/native-arm64-combined-assembly/local_build.py \
  --build --output /var/tmp/dispatcharr-arm64-local-build
```

The command builds the package and runs the native non-root wrapper smoke before keeping a random local image tag. It removes the owned intermediate images and containers, then retires the temporary build files. The output directory retains the private build log and text receipts. Pulled official base images may stay in the local image cache. Compose, release publication, and deployment are separate. The default application Dockerfile, AMD64 build command, React source, and tracked credits continue to use their existing inputs.

The current producer needs the official ARM64 runtime child `ghcr.io/dispatcharr/dispatcharr@sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace`. The controller verifies its original config, layer roster and architecture before using it. It also pulls the pinned official Rust, Node, and Debian ARM64 inputs recorded in the reviewed recipes. The initial acquisition uses the normal public Cargo and npm registries, the pinned Debian archives, Canonical's exact Ubuntu snapshot, and the checksum-bound Comskip upstream source archive. The locked Cargo archives, Debian package hashes, Ubuntu snapshot package hashes, and Comskip source hash are verified before use. The application package and generated rootfs come from this same local build. The repo provides the text pins and source, so no prior raw Docker-layer exports, credentials, or retained binary bundle are required. If an exact public input is unavailable, the build exits before keeping an image tag. [Official Dispatcharr image versions](https://github.com/orgs/Dispatcharr/packages/container/dispatcharr/versions?filters%5Bversion_type%5D=tagged)

The hosted [cf64 ARM64 run](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44896188921) verified the inherited assembly, normal Compose checks, and copied PostgreSQL workflow. This local entry and retention path have engine-free tests. Full credit belongs to the original Dispatcharr authors and dependency contributors. [Original attribution](../../../ATTRIBUTION.md)
