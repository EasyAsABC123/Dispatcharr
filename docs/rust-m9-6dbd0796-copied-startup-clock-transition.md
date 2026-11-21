# Copied-data startup clocks on 6dbd

On the exact `6dbd0796` package, the copied-data rehearsal preserved the identical preboot state. Original and Rust advanced the same four startup timestamps during their actual boots. The diagnostic verified complete non-clock data, schema, indexes, constraints and sequence equality at the measured boundaries. Authenticated readback and the edit on the owned Rust copy passed, as did exact fixture retirement. [Exact diagnostic receipt](../scripts/rust-integration/m9-6dbd0796-copied-startup-clock-transition.json)

The original strict comparator still exited 1. Its full fingerprint includes the four timestamps that change during ordinary Original startup. The separate startup-transition diagnostic exited 0. That evidence leaves the strict cold rollback gate open. [Strict operands and earlier result](rust-m9-strict-cold-startup-operands.md)

The first attempt reached the Native authenticated read and media comparison before the observer tried to capture boot readiness a second time during the edit's session recheck. Its controller exited 1 and retired its owned fixtures. The retry kept that authentication request and recorded each boot boundary once.

This is an isolated copied-data rehearsal. Production rollback, full M9 and comparative performance remain open. Original Dispatcharr and dependency contributors retain full credit. [Original attribution](../ATTRIBUTION.md)
