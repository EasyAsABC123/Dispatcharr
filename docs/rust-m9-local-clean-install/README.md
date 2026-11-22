# Replay the 9cf6407 Clean Install workflow locally

This replays the packaged native bootstrap, unchanged React first-admin and login, and restart checks with fresh PostgreSQL and Redis. The published [eleven-workflow evidence](../rust-m9-9cf6407-eleven-workflows.md) records the historical Clean Install result. A local rebuild uses the exact 2,157 application inputs and records its own image ID and results.

Use a disposable local Linux/AMD64 Docker Compose environment, Python 3.12 or later, Node.js 22 or later, and Chromium. Set `RUST_BROWSER_NODE` and `RUST_BROWSER_CHROMIUM` if those executables are outside `PATH`. Docker fetches pinned build and service images when they are missing.

Starting from a clone with the published migration branch and with this recipe's `verify.py` in the current directory, run:

```bash
set -eu
umask 077
replay_check="$(realpath verify.py)"
migration_repo=/path/to/Dispatcharr
replay_root="$(mktemp -d "${TMPDIR:-/tmp}/dispatcharr-m9.XXXXXXXX")"
checkout="$replay_root/checkout"
run="$replay_root/run"
application=9cf640701dacea07ea5f394ce7b18a6fa7c8c088
tools=a9a7e10f4ec636f9438836d046dba3f95b9e61ac

python3 -I -B "$replay_check" preflight --repository "$migration_repo"
git -C "$migration_repo" worktree add --detach "$checkout" "$tools"
mkdir "$run"
cd "$checkout"

build_exit=0
python3 -B scripts/rust-integration/build.py \
  --git-revision "$application" --tag "dispatcharr-rust-candidate:replay-${replay_root##*.}" \
  --timeout 1200 --output "$run/build" || build_exit=$?
printf '%d\n' "$build_exit" > "$run/build.actual-exit"
test "$build_exit" -eq 0
python3 -I -B "$replay_check" build --repository "$checkout" --output "$run"

clean_exit=0
python3 -B scripts/rust-integration/run_clean_install.py \
  --candidate-build-manifest "$run/build/build.json" --output "$run/clean-install" || clean_exit=$?
printf '%d\n' "$clean_exit" > "$run/clean-install.actual-exit"
test "$clean_exit" -eq 0
python3 -I -B "$replay_check" verify --repository "$checkout" --output "$run"
```

Preflight checks the application and tracked tool Git archives without Docker. The last command checks the build archive, local image binding, eleven required assertions, recorded process exit and absence of leftover Compose resources under the exact project label. Keep the raw logs, browser screenshot and generated build evidence in the private run directory.

Original Dispatcharr, React and dependency contributors retain full credit. See [upstream attribution](../../ATTRIBUTION.md).

The checker also verified all eleven assertions and empty Docker inventories against the retained physical `9cf6407` run. The replay command above will produce its own build and runtime receipts. [Retained-run verification](verification.json)
