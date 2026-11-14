#!/bin/bash
# Fixture-only management bootstrap; no Source web/worker/media process.
set -euo pipefail
umask 022
export PATH="/dispatcharrpy/bin:$PATH"
. /app/docker/init/01-user-setup.sh
export PG_VERSION PG_BINDIR
PG_VERSION="$(ls /usr/lib/postgresql | sort -V | tail -n 1)"
PG_BINDIR="/usr/lib/postgresql/$PG_VERSION/bin"
mkdir -p "$POSTGRES_DIR"
chown "$PUID:$PGID" "$POSTGRES_DIR"
chmod 700 "$POSTGRES_DIR"
su - "$POSTGRES_USER" -c "$PG_BINDIR/initdb -D '$POSTGRES_DIR' --encoding=UTF8 --locale=C.UTF-8"
. /app/docker/init/02-postgres.sh
configure_pg_network "$POSTGRES_DIR"
prepare_pg_socket_dir
su - "$POSTGRES_USER" -c "$PG_BINDIR/pg_ctl -D '$POSTGRES_DIR' start -w -t 120"
promote_app_role
ensure_app_database
redis-server --daemonize yes --protected-mode yes --bind 0.0.0.0 --port 6379 \
 --requirepass "$REDIS_PASSWORD" --save '' --appendonly no --dir /tmp
python manage.py migrate --noinput
python -c "from pathlib import Path;Path('/evidence/bootstrap-ready').write_text('migrations complete; no web/tasks/media')"
stop() {
 trap - TERM INT
 status=0
 redis-cli -h 127.0.0.1 -p 6379 --no-auth-warning SHUTDOWN NOSAVE || status=$?
 su - "$POSTGRES_USER" -c "$PG_BINDIR/pg_ctl -D '$POSTGRES_DIR' stop -m fast -w -t 20" || status=$?
 printf '%s\n' "$status" > /evidence/bootstrap-stop-status
 exit "$status"
}
trap stop TERM INT
while true; do sleep 1 & wait "$!" || true; done
