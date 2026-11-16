# Candidate33: invoke the packaged plugin host entrypoint

Original Dispatcharr, CPython, Django, Celery and dependency authors retain full
application, library and algorithm credit. React is unchanged.

The [SDK43 evidence](../scripts/rust-integration/milestone-batch33/inventory.json)
records actual zero exits for formatting, compilation, strict all-target Clippy
and workspace tests: **1,106 passed, zero failed and 783 ignored**, across 180
blocks. All 1,411 frozen inputs, including 1,405 Rust-tree files, are recorded.
Only `rust/runtime/plugin_bridge/dispatcharr_native/bootstrap.py` changed from
SDK42, and the installed file matches that tested snapshot exactly.

The production launcher executes `bootstrap.py` as a script, but its previous
version only defined `main()` and `run_action()`. It exited without starting the
RPC loop. Main33's first enabled Connect path reported a typed guide-effects
fault with no task result. The [correction and diagnosis](../scripts/rust-integration/milestone-batch33/components/diagnosis.json)
add the guarded invocation of the existing `main()`. Imported modules remain
inert; task parsing, transport, SQL ownership and shutdown deadlines are unchanged.

The [command-line controls](../scripts/rust-integration/milestone-batch33/CLIcontrols/inventory.json)
exercise the actual script with host Python and `--help`, which exits before
Django or native service setup. The old script exits0 with empty output and
fails the entrypoint check. The corrected script exits0 with 281 bytes of usage,
all declared options and no stderr. A separate actual Candidate32 production
launcher control also reproduces exit0 with empty output; its wait, EOF and exact
container absence are retained. The first package-control attempt failed in its
cleanup checker before persisting the child receipt, so its exit is not
reconstructed or accepted. A stale Main-container attempt was refused before
execution; Main33 completed naturally without SIGINT.

The [previous Candidate32 application evidence](../scripts/rust-integration/milestone-batch32/application/inventory.json)
remains separate: Source passes both fresh contexts; Native passes seven of ten.
Guide, M8 and held-guide contexts fail. The actual warm stop exits0 and its whole
restart/retry case passes. Both projects and all 15 recorded container identities
are cleaned up. Five guide-file observations used the wrong filesystem namespace;
held-guide prerequisites also need fixture corrections. These observations do
not turn the failed contexts into passes or establish a regex runtime defect.

Candidate33's Docker build, actual packaged launcher control, six package proof
families and full affected Native application rerun remain pending. In particular,
nonempty help output does not establish Django bootstrap, Connect delivery,
plugin execution or physical host shutdown. No full milestone or new performance
gain is accepted.
