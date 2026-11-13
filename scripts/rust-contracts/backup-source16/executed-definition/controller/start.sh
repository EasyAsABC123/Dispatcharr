#!/bin/bash
# UNEXECUTED owned PG/Redis bootstrap; real owned worker starts separately.
set -euo pipefail
export PATH="/dispatcharrpy/bin:$PATH"
. /app/docker/init/01-user-setup.sh
export PG_VERSION
PG_VERSION="$(ls /usr/lib/postgresql | sort -V | tail -n 1)"
export PG_BINDIR="/usr/lib/postgresql/$PG_VERSION/bin"
mkdir -p "$POSTGRES_DIR"
chown "$PUID:$PGID" "$POSTGRES_DIR"
chmod 700 "$POSTGRES_DIR"
su - "$POSTGRES_USER" -c "$PG_BINDIR/initdb -D '$POSTGRES_DIR' --encoding=UTF8 --locale=C.UTF-8"
. /app/docker/init/02-postgres.sh
prepare_pg_socket_dir
su - "$POSTGRES_USER" -c "$PG_BINDIR/pg_ctl -D '$POSTGRES_DIR' start -w -t 120"
promote_app_role
ensure_app_database
redis-server --daemonize yes --protected-mode yes --bind 127.0.0.1 --port 6379 --requirepass "$REDIS_PASSWORD"
python /backup-tools/source_fixture.py migrate
python -c "from pathlib import Path; Path('/evidence/bootstrap-ready').write_text('migrations exited; no web/worker yet')"
# Docker stop owns and reaps this service's PID namespace; the exact project is
# removed in outer finally, including PG/Redis and the shell's owned children.
exec sleep infinity
