# Candidate 87 copied rollback and Original restart SQL changes

Original Dispatcharr, PostgreSQL and dependency authors retain full application and tool credit.

The instrumented Candidate 87 cutover copied the pinned Original PostgreSQL fixture to a disposable sibling database and ran Native on the same loopback front door. The retained Original database had zero row differences through Native use and shutdown. The Native edit remained in the sibling copy. After rollback, the original authenticated session, channel and media worked on a fresh Original web generation. The paired launcher and Compose teardown exited 0. All five exact owned containers were absent, and the owned container, network and volume inventories were empty.

An independently verified Original-only restart observed the same four timestamp updates between quiescence and readiness as the instrumented rollback. The following authenticated channel read generated no additional row differences in either run. Schema and sequences matched.

| Table | Changed column | Rows |
| --- | --- | ---: |
| `core_systemnotification` | `updated_at` | 1 |
| `django_celery_beat_periodictask` | `date_changed` | 2 |
| `django_celery_beat_periodictasks` | `last_update` | 1 |

The strict full-row fingerprint after restart differed in both the new and historical Candidate 87 rehearsals. The new control localizes the difference for the instrumented run. The earlier receipt retained no post-restart row detail. The first instrumented attempt exited 1 during the read-only project inventory after its default sandbox refused the local Docker socket. It stopped before Compose project creation and has a separate terminal receipt. This qualifies the local copied rollback scenario while full M9, production rollout and performance remain open.

- [Original-only control](../scripts/rust-integration/cutover-candidate87-row-attribution/original-control.json)
- [Instrumented paired result](../scripts/rust-integration/cutover-candidate87-row-attribution/paired-rollback.json)
- [Sandbox refusal](../scripts/rust-integration/cutover-candidate87-row-attribution/sandbox-refusal.json)
- [Exact identities and approved digests](../scripts/rust-integration/cutover-candidate87-row-attribution/authority.json)
- [Historical scoped rehearsal](../scripts/rust-integration/cutover-candidate87-scoped.json)

Verify the portable receipts from a checkout with Python.

```bash
python3 -B scripts/rust-integration/cutover-candidate87-row-attribution/verify.py
```
