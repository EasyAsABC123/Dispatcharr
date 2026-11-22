# Explicit saved cleanup scheduler and retained queue restart

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

The ordinary Rust package provides an explicit process mode, `--saved-cleanup-scheduler <saved name>`, for one configured cleanup schedule. It claims the same PostgreSQL role lock as Beat. It checks the saved row and its schedule under a transaction before publishing. A confirmed broker reply permits one conditional checkpoint. The process reports an uncertain task ID if publication or checkpoint outcome is ambiguous. Reconcile that task before restarting this optional role.

In the owned Compose check, the first Native receiver retired before the schedule was armed. The producer published once and retired cleanly. A replacement producer loaded the same saved row and durable count from a new PostgreSQL session. The pending queue head and task ID remained unchanged during both transitions. A replacement receiver consumed the same task and returned `SUCCESS/null`. Two producer and two receiver generations exited zero, and all six owned container identities and process generations retired. The first attempt took its comparison snapshot before normal worker setup removed a dormant backup row. The retry verified that startup transition and took the comparison snapshot before arming the saved cleanup.

This check covers a healthy planned restart with an explicit saved schedule. Crash replay, ambiguous cross-system recovery, and default worker parity retain separate gates. [Machine-readable evidence](../scripts/rust-integration/m6-5d9568f-retained-producer.json)
