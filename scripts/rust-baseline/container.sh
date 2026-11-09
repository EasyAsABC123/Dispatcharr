#!/bin/bash
set -euo pipefail
export PATH="/dispatcharrpy/bin:$PATH"
python /verification/dependencies.py | tee /evidence/dependencies.json
# su - in the baseline bootstrap discards locale and initdb otherwise picks
# SQL_ASCII, incompatible with its later CREATE DATABASE ENCODING UTF8.
# Prepare only the ephemeral test cluster; application source stays untouched.
export PUID=1000 PGID=1000
. /app/docker/init/01-user-setup.sh
pg_version="$(ls /usr/lib/postgresql | sort -V | tail -n 1)"
mkdir -p "$POSTGRES_DIR"
chown "$PUID:$PGID" "$POSTGRES_DIR"
chmod 700 "$POSTGRES_DIR"
su - "$POSTGRES_USER" -c "/usr/lib/postgresql/$pg_version/bin/initdb -D '$POSTGRES_DIR' --encoding=UTF8 --locale=C.UTF-8"
# This invokes the existing CI bootstrap unchanged, with internal PG/Redis.
bash /app/scripts/ci_bootstrap_backend.sh "$@" -v2
# --keepdb preserves the migrated test database for runtime contract extraction.
export POSTGRES_DB="test_${POSTGRES_DB}"
python /verification/runtime_inventory.py > /evidence/runtime-contracts.json
