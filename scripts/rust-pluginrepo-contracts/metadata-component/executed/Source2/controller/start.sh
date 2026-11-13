#!/bin/bash
# Source-only PG/Redis bootstrap. All Django setup follows actual shell marker.
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
python manage.py shell -c "from dispatcharr.app_initialization import should_skip_initialization; assert should_skip_initialization() is True; from django.core.management import call_command; call_command('migrate',interactive=False,verbosity=0)"
python -c "from pathlib import Path; Path('/evidence/bootstrap-ready').write_text('management-shell migrations exited; no web/workers/media/plugin discovery')"
exec sleep infinity
