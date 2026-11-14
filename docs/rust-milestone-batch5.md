# Producer metadata and database environment compatibility

Original Dispatcharr and dependency authors retain all application, algorithm
and license credit. Celery and Kombu library notices are retained in source and
the Docker runtime. React remains unchanged.

The combined12-path,935-input candidate passes all four workspace gates:
**896 tests passed, zero failed,751 ignored**, across172 result blocks. The
[SDK evidence](../scripts/rust-integration/milestone-batch5/final-summary.json)
binds the exact input hashes and actual exits. Two test functions check72 pinned
Celery representation vectors and three Kombu OID vectors. Failed initial
compilation and strict-Clippy attempts remain captured; no checks were disabled.

The Core producer now derives tuple/dictionary representations from actual JSON
arguments, emits the six missing Source headers and computes Source-style
UUIDv3 reply identity using a process-random node and actual process, thread and
shared application identity. Origin uses the real process/hostname. Existing
admission and retained broker ownership remain in use. Actual Main Core99
acceptance is still required on the new committed image; these SDK results do
not rewrite the preceding five-frame gap.

Database configuration translates Original PostgreSQL TLS mode and CA/client
certificate/key variables, validates supplied files, handles explicit/default
Unix socket paths and unbracketed IPv6, and preserves the existing explicit URL
override. Unix sockets skip TLS; an explicit CA makes require use certificate
validation, following the [PostgreSQL17 connection rules](https://www.postgresql.org/docs/17/libpq-connect.html).
The Docker working directory becomes `/app`, matching pinned Original and its
relative paths. Runtime TLS/UDS and mode fallback behavior remain to be verified;
SQLx's allow/prefer retry behavior is a separate compatibility concern.

`DB_ENGINE=sqlite` now fails explicitly before selecting a PostgreSQL URL or
initializing a database. This prevents silent selection of another backend;
**SQLite is still unimplemented**, rather than accepted as migrated. The
[datastore scope](rust-datastore-compatibility.md) remains required.

The [Docker build and physical probes](rust-milestone-batch5-build.md) pass.
That one image must verify the Core99 producer change, affected job chains,
React regression and new PostgreSQL configuration cohort. New queue/scanner/DVR
proposals are excluded from this candidate until their actual common interfaces
and Main composition are ready. No full milestone or performance gain follows
from this SDK checkpoint.
