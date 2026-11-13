# Image57 immutable build evidence

Run `python3 scripts/rust-integration/build.py --git-revision d77f25fa94ee128c79ade9176d8eb4a9d23bf498 --output /tmp/dispatcharr-image57-replay --timeout 600 --tag dispatcharr-rust-candidate:image57-replay` with the documented Rust/Docker tooling. The original normalized archive is represented by its exact per-file map, Git revision and SHA256 in build.json; no binary dependency cache or image layer is committed here. Main/SDK/package/performance results require their own evidence.

Root independently checked all 844 compiled Git blobs before the build, the 1,336 application inputs, 114 installed frontend/license files against image56, and exact probe-container absence. The probe runs read-only as the image UID65532 with network disabled. Original authors retain full application credit.
