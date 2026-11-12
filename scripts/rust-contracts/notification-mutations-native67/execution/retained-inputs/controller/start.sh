#!/bin/bash
# Unexecuted owned Native-QA storage bootstrap. No Django/Source app runs.
set -euo pipefail
export PATH="/dispatcharrpy/bin:$PATH"
. /pg-bootstrap/01-user-setup.sh
export PG_VERSION
PG_VERSION="$(ls /usr/lib/postgresql | sort -V | tail -n 1)"
export PG_BINDIR="/usr/lib/postgresql/$PG_VERSION/bin"
[ "$PG_VERSION" = 17 ]
mkdir -p "$POSTGRES_DIR"
chown "$PUID:$PGID" "$POSTGRES_DIR"
chmod 700 "$POSTGRES_DIR"
su - "$POSTGRES_USER" -c "$PG_BINDIR/initdb -D '$POSTGRES_DIR' --encoding=UTF8 --locale=C.UTF-8"
. /pg-bootstrap/02-postgres.sh
prepare_pg_socket_dir
su - "$POSTGRES_USER" -c "$PG_BINDIR/pg_ctl -D '$POSTGRES_DIR' start -w -t 120"
promote_app_role
ensure_app_database
redis-server --daemonize yes --protected-mode yes --bind 127.0.0.1 --port 6379 --requirepass "$REDIS_PASSWORD"
# Initialize only this disposable container's default/sentinel via exact Native
# schema SQL so eleven named tables/sequences exist for read-only audits.
# The target remains a separate empty DB for Rust's initializer inside its QA.
psql -Xq -h /var/run/postgresql -U dispatch -d dispatcharr -v ON_ERROR_STOP=1 -1 \
  -f /native-source/rust/crates/store/schema/original-1241b3-schema.sql \
  -f /native-source/rust/crates/store/schema/original-1241b3-defaults.sql \
  -f /native-source/rust/crates/store/schema/original-1241b3-fresh-timestamps.sql
printf '%s\n' 'PG17/Redis ready; sentinel Native schema loaded; no application started' > /evidence/bootstrap-ready
exec sleep infinity
