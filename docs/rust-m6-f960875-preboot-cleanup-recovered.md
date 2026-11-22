# Copied Cleanup checkpoints on ordinary AIO f960875

The original Dispatcharr, Django and Celery authors and contributors retain full credit. See [Attribution](../ATTRIBUTION.md).

The current-cycle recipient completed its physical run and proof with zero exits. It preserved its saved Cleanup checkpoint without a task publication. The separate overdue recipient produced one Cleanup result with SUCCESS and a null payload. The same saved row advanced from count one to two and held that value while the modular web peer continued serving after the ordinary AIO worker retired.

The overdue controller returned 1 after both roles stopped normally. Its terminal parser expected the Redis timestamp under a different field name. An independent read-only reduction of all 1,548 fsynced journal records reconciled both startup roots, the one Cleanup root, the scoped SQL and the exact retirement of all six owned roles. That reducer returned 0 using the same measured run. The original physical exit of 1 remains in the evidence.

The [machine-readable proof](../scripts/rust-integration/m6-f960875-preboot-cleanup-recovered.json) retains the completed current-cycle and observed overdue checkpoints. A fresh whole-row pass and cold Original counterpart remain open.
