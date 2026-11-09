#!/bin/bash
# Capture only fresh original migration/default state; no playback/user seed.
set -euo pipefail
export PATH="/dispatcharrpy/bin:$PATH"
[ "$POSTGRES_PASSWORD" = playback-fixture-only ]
. /app/docker/init/01-user-setup.sh
export PG_VERSION
PG_VERSION="$(ls /usr/lib/postgresql | sort -V | tail -n 1)"
export PG_BINDIR="/usr/lib/postgresql/$PG_VERSION/bin"
mkdir -p "$POSTGRES_DIR"
chown "$PUID:$PGID" "$POSTGRES_DIR"
chmod 700 "$POSTGRES_DIR"
su - "$POSTGRES_USER" -c "$PG_BINDIR/initdb -D '$POSTGRES_DIR' --encoding=UTF8 --locale=C.UTF-8" > /evidence/initdb.log
. /app/docker/init/02-postgres.sh
prepare_pg_socket_dir
su - "$POSTGRES_USER" -c "$PG_BINDIR/pg_ctl -D '$POSTGRES_DIR' start -w -t 120" >> /evidence/initdb.log
promote_app_role
ensure_app_database
redis-server --daemonize yes --protected-mode yes --bind 127.0.0.1 --port 6379 --requirepass "$POSTGRES_PASSWORD"
cd /app
python manage.py migrate --noinput > /evidence/migrations.log 2>&1
python /schema-tools/capture_clean_schema.py > /evidence/defaults.json
export PGPASSWORD="$POSTGRES_PASSWORD"
"$PG_BINDIR/pg_dump" -h /var/run/postgresql -U "$POSTGRES_USER" -d "$POSTGRES_DB" --schema-only --no-owner --no-acl > /evidence/schema.raw.sql
"$PG_BINDIR/pg_dump" -h /var/run/postgresql -U "$POSTGRES_USER" -d "$POSTGRES_DB" --data-only --column-inserts --no-owner --no-acl > /evidence/defaults.raw.sql

if [ "${SCHEMA_REFERENCE_SERVE:-0}" = 1 ]; then
  exec python /verification/supervisor.py serve
fi
