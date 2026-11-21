# Default Rust Main active standard provider cadence

Pinned Original and packaged Rust 814b9403 each completed two normal
administrator cadence changes on an active standard provider with an
existing enabled interval. A GET, one-field PATCH and GET changed four
hours to seven, then seven back to four. All six responses were 200 in
each arm, and only the interval changed in the typed response.

The enabled task retained its identity and run history. The shared
four-hour interval stayed intact for the control account and the
unreferenced seven-hour interval retired after the second edit.
The schedule and Beat marker clocks advanced within the request
windows. The readbacks added no SQL mutations. Both broker observers
recorded zero new queue admissions or task-result stores and retained
their existing result-key sets.

Stock used its web and Celery fixture with Beat stopped. Rust used
ordinary Main with Selected jobs, Beat, EPG and Version flags off.
Rust Main exited zero, and both projects retired all five owned
containers and observed generations with empty project inventories.
The first two pairs exited one after Stock completed. Rust fixture setup
first rejected the Native role, then required a live Main process before
the copied database was seeded. Both projects retired cleanly in each
attempt. The passing pair used the same package and seeded the fixture
before its declared Main startup.

Schedule execution, the full M6 worker graph and performance have
separate gates.
[Redacted cadence receipt](../scripts/rust-integration/m6-ordinary-active-std-cadence.json)

The original Dispatcharr, Celery and Django Celery Beat authors and
contributors retain full credit. [Attribution](../ATTRIBUTION.md)
