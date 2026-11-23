# Pinned Original Python CI baseline rerun

Original Dispatcharr and dependency authors retain full credit for the application, tests, and dependencies under their existing licenses.

The unchanged Original backend at `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` ran in the exact cached linux/amd64 image `sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f`. The existing CI discovery returned the same 16 labels before launch and in the container. The unchanged CI bootstrap discovered and passed all 3,362 tests. All 31 direct dependency checks passed. The exported inventory contains 592 resolved Django patterns and 246 public OpenAPI paths with 362 operations. The dependency and inventory files match the [prior verified baseline](../scripts/rust-baseline/verified-full-baseline.json) byte-for-byte. Existing schema fallback diagnostics still identify views omitted from OpenAPI.

The one Compose container ran with networking disabled, no published ports, no package sync, read-only source and three disposable tmpfs mounts. All six launcher phases and the container's physical exit were `0`. Precisely one owned container and zero owned networks or volumes existed before removal. The same engine reported each owned resource class empty afterward. An independent audit matched the retired container ID in the saved receipts, rechecked the exact 1,211-file source archive and census, and recomputed the retained artifact hashes. [Sanitized receipt](../scripts/rust-baseline/verified-original-ci-baseline-rerun-1.json)

Two earlier attempts used the same Original source and image and exited `1` in the unchanged read-only plugin test, with the privilege-dropped child unable to import `dispatcharr` and `dispatcharr.settings_test`, respectively. The later verified pass and this rerun used a traversable source mount and retained that test. Both earlier failure logs and exit results retain their separate hashes in the receipt.

This qualifies the pinned Original backend baseline portion of M0. The executable Source contract matrix, complete M0 acceptance, Rust application parity, and performance remain separate gates.
