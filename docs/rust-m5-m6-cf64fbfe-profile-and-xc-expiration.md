# Channel Profile rendering and XC expiry on cf64fbfe

Both packaged backends passed 28 explicit Channel Profile JSON reads, plus four compact controls each. The ordinary list and detail requests covered administrators and assigned Standard users, wildcard media ranges, a more specific compact choice, parameter whitespace, zero and negative indentation, and a mixed HTML and JSON Accept header. The selected JSON renderer and typed payload matched the pinned Original fixture. Owned PostgreSQL rows stayed unchanged.

The paired XC account refresh produced a successful broker root and a natural profile child on each backend. Both stored the minute-precision expiry from the owned provider response as 2031-06-02 10:55 UTC and kept the existing Unix timestamp control. The same linked hourly Beat row stayed disabled at zero account cadence on each backend. Owned lifecycle and project cleanup passed.

The first controller exited 1 on Original before sending the refresh request. Its fixture incorrectly expected zero cadence to have no linked row. A stale callback also failed during that attempt. A separate queued launch stopped before Docker because an independent SQLite report reducer failed. The fresh retry checked the row's identity, interval, and modification clock before and after the refresh. The receipt records the first controller's failure and its independent cleanup alongside the successful retry.

The exact Rust source passed formatting, strict Clippy, and 1,497 enabled Cargo tests. Wider negotiation, worker graph, integrated parity, and comparative performance remain open. [Exact package and paired receipts](../scripts/rust-integration/m5-m6-cf64fbfe-profile-and-xc-expiration.json)

Original Dispatcharr, Django REST Framework, Celery, and dependency authors and contributors retain full credit. [Attribution](../ATTRIBUTION.md)
