# Actual Python baseline in Docker Compose

```sh
python3 scripts/rust-baseline/run.py --output /tmp/dispatcharr-python-baseline
```

Requires Git, Python 3.9+, tar, Docker Engine, and Compose v2. `--docker` accepts
an alternate Docker executable; normal `DOCKER_HOST`/`DOCKER_CONFIG` environment
variables are honored. Use a fresh empty output directory for every run. Optional
positional Django test labels replace the focused defaults.
`--timeout` sets a finite run limit (default 1,800 seconds). Each invocation uses
a unique project/container name and removes only its own container in `finally`,
including timeout and interruption paths. `run.json` retains failure status and
invoked command plus tool hashes; cleanup errors are explicit. Failure-path tests
run without Docker: `python3 -m unittest discover -s scripts/rust-baseline -p 'test_*.py'`.

The runner archives commit `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` directly
from Git, including the baseline CI scripts. Uncommitted working tree changes are
reported but never enter the archive. The container mounts it read-only and runs
the existing `scripts/ci_bootstrap_backend.sh` unchanged. It installs no packages,
publishes no ports, uses no external network, and retains no database volume.
PostgreSQL, Redis, `/data`, media, and `/tmp` are disposable container state.

The prebuilt CI image is pinned to its linux/amd64 manifest digest in
`docker/compose.rust-baseline.yml`. On another architecture, emulation would change
performance and requires separate evidence. Do not use these tests as a cross-host
performance comparison.

The image's direct dependency versions are checked against the archived
`pyproject.toml`; incompatibility fails before tests rather than silently syncing
dependencies. The image digest also fixes transitive dependencies. PostgreSQL is
preinitialized with UTF-8 because the existing bootstrap's `su -` drops locale
variables and otherwise picks SQL_ASCII, incompatible with its later UTF-8
database creation. This is test-environment preparation, not a baseline code fix.

Default tests cover existing TS discontinuity helper behavior, source-switch
buffer behavior, and native HLS segmentation. After they pass, the preserved
migrated test DB supports actual Django route and public OpenAPI extraction.

Artifacts:

* `run.json`: commit, immutable image, test labels, excluded working-tree changes,
  timestamp, and combined test/inventory exit status.
* `dependencies.json`: installed direct dependencies and source compatibility.
* `tests.log`: bootstrap, migration, test results, and schema-generation warnings.
* `runtime-contracts.json`: resolved route patterns and runtime OpenAPI schema.

Runtime introspection includes Django admin and aliases. OpenAPI generation emits
existing warnings and graceful serializer-inference errors that omit views, and
does not specify all streaming/operational behavior. Route
discovery is not an executable contract suite. This focused run does not complete
M0 or prove full backend correctness, playback parity, or any performance gain.

The successful local run is summarized in `verified-baseline.json`. Re-run to
regenerate full artifacts; compare semantically because test timings, DB logs,
timestamps, and other volatile fields make file hashes run-specific.
