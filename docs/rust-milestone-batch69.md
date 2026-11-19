# Parallel Rust migration checkpoint

Original Dispatcharr and dependency authors retain credit for the application,
models, algorithms and protocols.

This checkpoint groups independent parity changes across four lanes.
Development continues in parallel across milestones. The integrated release
candidate will receive the full acceptance run.

| Lane | Change | Focused evidence |
| --- | --- | --- |
| M1–M4 playback | Apply Requests CA configuration to live HTTP, VOD and timeshift playback. An invalid bundle still permits plain HTTP. TLS verification rejects peers without trusted roots, including peers reached by redirects. | A loopback comparison against pinned Requests matched 16 client and trust combinations. |
| M5 and M7 interfaces | Channel ranges accept finite Python float spellings and route HEAD through the same authorization and lookup. Direct M3U preserves malformed stored UDP URLs and marks valid multicast addresses. | Rust range and mixed-playlist tests passed. An isolated Axum test checked HEAD status, headers and empty body. |
| M6 providers and jobs | Profiles use a configured user agent before consulting defaults. A fallback uses the admitted SQL session. XC refresh preserves elapsed-time precision and error event field order. | Strict compilation passed. The full run will check Redis writes, both user-agent branches and WebSocket data through Main. |
| M0, M8 and M9 operations | Backup downloads encode Unicode filenames as Django does. Release workflow acceptance depends on verified SQLite and PostgreSQL scope. | The Rust backup test and 1,005 filename comparisons passed. The coordinator test verified that unqualified datastore scope blocks dispatch of the release workflow. |

Workspace formatting and strict linting across all affected Rust crates passed.
The three targeted Rust regressions passed in the shared workspace.
[Check receipt](../scripts/rust-integration/milestone-batch69/parallel-wave-1.json)

A second group addresses HLS cold-start, HDHomeRun and XC failure paths. HLS
polls channel and output state together, returns 410 for an already stopped
cold stream, and gives a ready channel its own playlist deadline. The clock
matched 2,412 observations from the pinned Python helper. An isolated Redis
test matched 144 states, including stopped streams with a pending descriptor.

HDHomeRun preserves literal ampersands in profile paths and returns an empty
lineup for an unknown profile before loading default output settings. XC
counts a profile failure after all four connection attempts have ended and
continues processing the remaining profiles. Store and capacity errors
retain their existing handling.

The combined Rust code passed workspace formatting, strict compilation
including Main, and three focused tests for HLS, HDHomeRun and XC.
[Second check receipt](../scripts/rust-integration/milestone-batch69/parallel-wave-2.json)

The [milestone coordinator](../scripts/rust-milestone-batches/README.md) still
requires complete case bindings and an exact release image. The final run
covers functional Compose workflows, paired performance, a two-hour soak, and
rollback on copied data. Runtime parity and full milestone closure remain open.
