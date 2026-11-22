# Named Cleanup checkpoints on e68fb95

The original Dispatcharr and original Django and Celery authors retain full credit. See [Attribution](../ATTRIBUTION.md).

Three independent ordinary AIO Compose fixtures used named daily Cleanup rows with an untouched sibling. The saved schedule name, row identity, sibling checkpoint, closed Redis journal and owned teardown were verified in each run.

| Target checkpoint | Named Cleanup roots | Durable target count |
| --- | ---: | ---: |
| Copied current-cycle checkpoint at boot | 0 | 2 |
| Copied overdue checkpoint at boot | 1 | 3 |
| Checkpoint made overdue after readiness | 1 | 3 |

Both overdue cases returned SUCCESS with a null result and advanced the persisted target once. The independent proofs returned 0, and each run preserved the sibling and retired its owned resources exactly. The retained Original named Cleanup proof establishes an actual Original execution in its own context.

The [machine-readable results](../scripts/rust-integration/m6-e68fb95-named-cleanup-checkpoints.json) qualify this supported daily named shape. The matched Original checkpoint-at-boot variants, wider saved schedule shapes, and full M6 remain open.
