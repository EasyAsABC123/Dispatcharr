# Packaged PostgreSQL blank defaults

Original Dispatcharr authors and contributors retain full application credit. Original's pinned Docker entrypoint maps blank `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PORT` to `dispatcharr`, `dispatch`, and `5432`.

This runner compares the exact Candidate86 and Candidate87 resolver bytes. It runs nine cells against the shipped operator, including individually blank fields, all three blank, and explicit `DATABASE_URL` precedence. Each cell checks the effective non-secret settings, the exact local image and operator executable, and an actual SQL-backed command. Distinct ambient PostgreSQL arguments detect accidental fallbacks.

The tracked clean-install Compose configuration supplies pinned temporary PostgreSQL and Redis services. The runner loads only those dependencies and the one-off operator containers within its own internal network. It generates PostgreSQL and Redis passwords at runtime. The saved Compose overlay contains placeholders, and the result includes exact exits, scoped observations and cleanup.

From a checkout with this fixture, run the offline controls or extract the same files as a small portable fixture:

```bash
python3 -B scripts/rust-operator-contracts/test_blank_postgres.py

git archive "$fixture_revision" \
  LICENSE ATTRIBUTION.md docker/compose.rust-clean-install.yml \
  scripts/rust-integration/build.py scripts/rust-milestone-batches/functional_lease.py \
  scripts/rust-operator-contracts/run_blank_postgres.py \
  scripts/rust-operator-contracts/test_blank_postgres.py \
  scripts/rust-operator-contracts/blank-postgres-defaults.md | tar -x -C "$export_directory"
```

Keep each passing build manifest beside its existing source archive. The fixture validates those archives in place and uses the packaged schema. Have the two candidate images and the pinned database/cache images available on the explicit local validation Docker socket. For an exported fixture sharing a validation engine, use the active checkout's lease root.

```bash
python3 -B "$export_directory/scripts/rust-operator-contracts/run_blank_postgres.py" \
  --docker "$docker_cli" --docker-host "$owned_docker_socket" \
  --lease-root "$migration_checkout" \
  --before "$candidate86_manifest" --before-revision a1e2231b176508355e4b1afd29a0b82a3e74fb57 \
  --after "$candidate87_manifest" --after-revision 7aef18c76b911e44fef7e42e4211280190f70daf \
  --output "$new_evidence_directory"
```

Use a socket value such as `unix:///tmp/dispatcharr-docker.sock`. The fixture retains the exclusive lease until scoped Compose cleanup, project inventories and exact container absence are verified.
