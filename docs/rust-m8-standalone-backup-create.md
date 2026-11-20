# Explicit standalone manual PostgreSQL backup

The original Dispatcharr, Celery and PostgreSQL authors retain full credit.

Ordinary Rust Main can run manual PostgreSQL backup create and task status
when `RUST_BACKUP_CREATE_OWNER=rust` is configured with explicit
`CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND`. Assign that namespace to
a single manual worker, separate from other producers and schedulers.
This configuration starts no scheduler or pidbox control receiver. The
default keeps its existing backup behavior.

The exact `a785a2b7` package and pinned Original ran in separate owned
Compose projects. Authenticated create returned HTTP 202. Both signed
task status responses matched an actual Redis SUCCESS result, and both
authenticated downloads returned HTTP 200. Each archive matched the
format-2 ZIP member and metadata contract, began its database member with
the PostgreSQL custom-dump signature, and matched the worker's recorded
length. The representative seeded SQL snapshot and independent control
stayed unchanged in both projects.

The Original Celery worker and Rust Main retired normally after completion.
The controller exited 0 and independently verified retirement of both
Original Compose generations and all four Native generations. The projects
had no remaining containers, networks or volumes.

Two earlier runs of the same image completed the archive operations and
exited 1 when Native Main stopped. The clean run used Django's management
shell for its SQL inspection so application startup hooks were skipped.
The Native broker's backup queue was empty before and after the snapshot.
The earlier queue contents were unobserved.

The 18 focused host tests included configuration with the option absent
and with a competing worker. They verified pre-claim capacity retry,
create-only transport authority and preservation of worker failure during
shutdown retries. Strict all-target Clippy exited 0. The paired Compose
run used the option enabled and verified a normal stop after completion.
Live timezone changes, a shutdown deadline with work still active, restore,
scheduled jobs, cancellation, full M8 and performance remain open.

[Redacted paired receipt](../scripts/rust-integration/milestone-batch113/standalone-backup-create-a785.json)
