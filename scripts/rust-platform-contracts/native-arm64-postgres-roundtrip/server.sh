#!/bin/sh
set -eu
export LC_ALL=C TZ=UTC
bin=/usr/lib/postgresql/17/bin
data=/tmp/owned-pg-data
[ "$(id -un)" = postgres ]
[ "$(uname -m)" = aarch64 ]
[ "$(dpkg-query -W -f='${Version}' postgresql-17)" = '17.11-1.pgdg26.04+2' ]
case "$("$bin/postgres" --version)" in
    'postgres (PostgreSQL) 17.11'|'postgres (PostgreSQL) 17.11 '*) ;;
    *) exit 1 ;;
esac
test -x "$bin/initdb" && test -x "$bin/pg_isready" && test ! -e "$data"
mkdir -m 0700 "$data"
"$bin/initdb" -D "$data" --encoding=UTF8 --locale=C --no-sync \
    --username=fixture_owner --auth-local=trust --auth-host=scram-sha-256 \
    --pwfile=/run/owned-pg-password > /tmp/owned-init.log 2>&1
exec "$bin/postgres" -D "$data" -p 5432 -c 'listen_addresses=*' -c unix_socket_directories=/tmp \
    -c max_connections=12 -c shared_buffers=16MB -c logging_collector=off -c ssl=off
