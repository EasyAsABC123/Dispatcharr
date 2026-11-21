#!/bin/sh
set -eu
phase=identity
failed_phase() {
    state=$?
    trap - EXIT
    if [ "$state" -ne 0 ]; then
        printf 'owned-postgres-roundtrip-phase:%s\n' "$phase" >&2
    fi
    exit "$state"
}
trap failed_phase EXIT
export LC_ALL=C TZ=UTC PATH=/usr/local/bin:/usr/bin:/bin
export PGHOST=owned-pg PGPORT=5432 PGUSER=fixture_owner PGPASSWORD="$(cat /run/owned-pg-password)"
export PGCONNECT_TIMEOUT=5 PGSSLMODE=disable
unset LD_LIBRARY_PATH LD_PRELOAD PGSERVICE PGHOSTADDR
[ "$(id -u)" = 65532 ] && [ "$(uname -m)" = aarch64 ]
[ ! -e /usr/lib/postgresql/17/bin/postgres ]
docs=/usr/share/doc/dispatcharr-standalone-postgres
phase=wrapper-integrity
sum=$(sha256sum "$docs/files.sha256")
[ "${sum%% *}" = 04456807c73b1c5898cda0bcdbe9d51447c97f8c67ea8bd1b6320964aff0b44c ]
sha256sum -c --strict "$docs/files.sha256" > /dev/null
phase=server-version
case "$(psql -XAtq -v ON_ERROR_STOP=1 -d postgres -c 'SHOW server_version_num')" in
    170011) ;;
    *) exit 1 ;;
esac
phase=create-databases
psql -Xq -v ON_ERROR_STOP=1 -d postgres -c 'CREATE DATABASE fixture_source'
psql -Xq -v ON_ERROR_STOP=1 -d postgres -c 'CREATE DATABASE fixture_restored'
phase=seed-table
psql -Xq -v ON_ERROR_STOP=1 -d fixture_source <<'SQL'
CREATE TABLE public.owned_notes (
    id integer PRIMARY KEY,
    note text NOT NULL,
    weight integer NOT NULL CHECK (weight > 0)
);
INSERT INTO public.owned_notes VALUES (1,'alpha',2),(2,'beta',3),(3,'gamma',5);
SQL
phase=source-rows
before=$(psql -XAtq -v ON_ERROR_STOP=1 -d fixture_source \
    -c "SELECT string_agg(concat_ws(':',id,note,weight), ':' ORDER BY id) FROM public.owned_notes")
[ "$before" = '1:alpha:2:2:beta:3:3:gamma:5' ]
phase=dump
pg_dump --no-owner --no-privileges --format=custom --compress=gzip:1 --file=/tmp/owned.dump fixture_source
phase=archive-size
test -s /tmp/owned.dump
size=$(wc -c < /tmp/owned.dump)
[ "$size" -gt 512 ] && [ "$size" -lt 262144 ]
phase=archive-catalog
pg_restore --list /tmp/owned.dump > /tmp/archive.list
phase=archive-expected-table
grep -q ' TABLE DATA public owned_notes ' /tmp/archive.list
phase=restore
pg_restore --single-transaction --exit-on-error --no-owner --no-privileges -d fixture_restored /tmp/owned.dump
phase=restored-rows
after=$(psql -XAtq -v ON_ERROR_STOP=1 -d fixture_restored \
    -c "SELECT string_agg(concat_ws(':',id,note,weight), ':' ORDER BY id) FROM public.owned_notes")
[ "$after" = "$before" ]
phase=constraints
for db in fixture_source fixture_restored; do
    shape=$(psql -XAtq -v ON_ERROR_STOP=1 -d "$db" -c \
        "SELECT string_agg(contype::text, ',' ORDER BY contype::text) FROM pg_constraint WHERE conrelid='public.owned_notes'::regclass")
    [ "$shape" = 'c,p' ]
done
phase=terminal-projection
archive=$(sha256sum /tmp/owned.dump)
printf '{"db_major_minor":"17.11","archive_bytes":%s,"archive_sha256":"%s","exact_three_rows":true,"exact_two_constraint_types":true,"single_transaction_restore":true,"retained_wrappers_sha256":"04456807c73b1c5898cda0bcdbe9d51447c97f8c67ea8bd1b6320964aff0b44c"}\n' "$size" "${archive%% *}"
