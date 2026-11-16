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

Candidate33's Docker build, actual packaged launcher control and six package
proof families below pass. The affected Native application rerun is running.
Nonempty help output does not establish Django bootstrap, Connect delivery,
plugin execution or physical host shutdown. No full milestone or new performance
gain is accepted.


## Candidate33 package and launcher checks

The [package inventory](../scripts/rust-integration/milestone-batch33/package/inventory.json)
binds runtime `32d5135dc084dd8656599cfedd800fc9473b55c9` to image
`sha256:985059bb98284d371a48fbf791dd49127c2ad20e1e9384db793a955133a2114f`.
The build actually exits0 after191.943 seconds. All six sequential package
checks exit0. The full Python-host check additionally invokes the installed
`dispatcharr-plugin-python --help` as UID65532: it emits281 nonempty usage bytes
including `--source`, `--plugins`, `--mode` and `--role`, unlike the retained old
empty-output result. This checks the actual production wrapper, not just an
import. It does not establish RPC handshake or plugin business execution.
Independent read-only inventories verify all54 exact package container IDs
absent after all six waits. React and Original/dependency credits are preserved.

The local build runner accidentally retained the `milestone32` tag literal.
The raw build manifest/log remain unchanged. An
[explicit alias receipt](../scripts/rust-integration/milestone-batch33/package/tag-alias.json)
verifies `dispatcharr-rust-candidate:milestone33` resolves to the exact new image
and source labels. Overwriting the old tag made the old32 image ID unavailable
locally; restoration failed, and the misleading32 tag was removed after checking
the33 reference remained intact. Historical32 evidence is unchanged. No rebuild
or source replay was performed. Reproduction uses the verified33 alias or exact
image ID, with the raw tag discrepancy retained.

The affected Native guide/M8/held application batch remains a separate gate.
No full milestone or new performance gain is accepted.
