#!/bin/bash
set -euo pipefail
export PATH="/dispatcharrpy/bin:$PATH"
python /baseline-tools/dependencies.py > /evidence/dependencies.json
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
redis-server --daemonize yes --protected-mode yes --bind 0.0.0.0 --port 6379 --requirepass "$REDIS_PASSWORD"
python manage.py migrate --noinput
python /verification/seed.py
exec python /verification/supervisor.py serve
