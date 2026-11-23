# ARM64 publication-observer attribution at 96 MiB

On Lappy, the isolated publication observer was OOM-killed and exited 137 at the 96 MiB limit. Both application roles exited 0. The observer did not reach prepared state, and the fixture started zero XMLTV jobs or viewers. The actual diagnostic exited 1 and exact cleanup exited 0.

The trace retained 129 complete records. The last completed callback was `apps.backups.ready()`. It entered `apps.channels.ready()` at 87.71 MiB charged, already above the required 86.4 MiB lifetime headroom. Only the before record survived for that callback. The last retained sample recorded one cap event. The later Docker terminal established the OOM. The terminal memory-event counters and the allocation responsible for the kill remain unobserved.

This isolates the observer startup gate from application resource measurements. The [256 MiB diagnostic](rust-arm64-publication-observer-setup-memory.md) measured a 110.12 MiB peak through disposal on the exact earlier application. The 96 MiB role cap and formal acceptance budget stay in force.

Original Dispatcharr, Django, React and dependency authors and contributors retain full credit. [Attribution](../ATTRIBUTION.md) · [Machine-readable evidence](../scripts/rust-integration/arm64-publication-observer-96mib-attribution.json)
