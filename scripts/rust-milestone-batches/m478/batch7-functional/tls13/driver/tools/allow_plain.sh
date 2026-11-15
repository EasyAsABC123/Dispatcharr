#!/bin/bash
# Owned fixture only: allow plaintext while retaining TLS support/client certs.
set -euo pipefail
cat > /data/db/pg_hba.conf <<'CONF'
local all all trust
hostnossl all all 127.0.0.1/32 trust
hostnossl all all ::1/128 trust
hostssl all all 127.0.0.1/32 cert
hostssl all all ::1/128 cert
CONF
su - dispatch -c '/usr/lib/postgresql/17/bin/pg_ctl -D /data/db reload'
