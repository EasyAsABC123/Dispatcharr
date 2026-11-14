# New Original PostgreSQL TLS settings/backend authority

All application/driver behavior credit belongs to the Original Dispatcharr,
Django, django-db-geventpool, psycopg and PostgreSQL authors. This is a new
configuration/physical-driver cohort, not an API comparison or performance test.

The pinned Original Source is mounted unchanged. `probe.py` imports Original
settings and its normal `core.apps` trace registration, then uses the actual
`dispatcharr.db.backends.postgresql_psycopg3` backend to execute a read-only
SELECT joining its own `pg_stat_ssl` session. No `django.setup`, AppConfig ready
hook, Source API or function replacement runs. Certificate keys exist only in
the owned Docker TLS volume; it is deleted during exact cleanup. Server SAN is
IP 127.0.0.1; authenticated client CN is dispatch.

Attempt1 preserved pre-script permission failures (no probe executed). Attempt2
preserved the missing normal trace-registration failures before physical PG
connect, and actually completed missing-CA configuration rejection and SQLite
SELECT. Attempt3 resumes only the remaining ten driver cases. No completed
Source business case was replayed for a green controller.

Attempt3 controller exit0 and exact cleanup0. It proves verify-full success,
wrong-CA refusal, hostname-mismatch refusal, verify-ca accepting that hostname
mismatch, client-certificate requirement, require+wrong-CA refusal, explicit UDS
ignoring SSL, IPv6 verify-ca success, and separate Source allow/prefer success.
Read-only before/after fingerprints cover all62 default tables/59 sequences and
Redis DB7 key count. Physical project/container/network/volume inventories and
host generation absence are captured, not inferred from Compose down alone.

These are configuration/driver observations only. Source SQLite support remains
a required Rust implementation gap. Source allow/prefer fallback behavior needs
separate comparison against SQLx; this Source capture does not establish Native
mode parity or production Main startup. No performance gain is claimed.
