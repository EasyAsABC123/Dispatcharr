# Smaller validation builds

Original Dispatcharr authors and contributors retain full application and
algorithm credit. Existing AGPL licensing is preserved.

The workspace now defaults to `debug = 0` and `incremental = false` for development
and test profiles. Separate frozen verification runs previously repeated large
debug sections and incremental caches. Recorded evidence binaries, source and
results belong outside disposable Cargo output.

A committed 565-input checkpoint was frozen with only this workspace profile
change. Strict workspace/all-target Clippy and all 356 default tests pass;
361 external-fixture tests remain explicitly ignored. All 153 observed local
compiler profiles retain debug assertions and overflow checks, with optimization
level zero and no debug information. The target contains no incremental files.

The same strict-check/test workload produced 3,113,185,466 apparent bytes instead
of 24,410,853,855: **87.2% less build output** (2.9 GiB versus 22.7 GiB). This is a
build-output comparison; it does not measure application CPU or working memory.
The source, commands and toolchain are bound in the
[verification report](../scripts/rust-integration/verified-build-output-cleanup-1.json).

Use the normal Cargo commands. For an investigation requiring full debug
information, explicitly opt in for that run:

```sh
CARGO_PROFILE_DEV_DEBUG=2 CARGO_PROFILE_TEST_DEBUG=2 cargo test \
  --manifest-path rust/Cargo.toml -p dispatcharr-job-transport \
  --test interval_schedule
```

Keep active targets announced and protected. After verification, retain frozen
source, input manifests, logs/results and specifically recorded evidence binaries,
then remove completed generated output using the
[retention procedure](rust-build-output-retention.md). The completed cleanup
removed 895.4 GiB of apparent outputs and increased measured filesystem free
space by 332.2 GiB. Source and evidence remain available; regeneration of removed
Cargo products requires a rebuild.
