# cf64 production MPEG-TS checks

Original Dispatcharr authors and contributors retain full application, workflow and algorithm credit.

The exact `cf64fbfe776c` package passed three of the 127 required
coordinator nodes. Each used the production Rust Main entrypoint in its own
isolated Compose fixture. The coordinator exited 2 with 124 blocked because
of missing bindings or unaccepted dependencies. The enclosing sequence
exited 0 after verifying the three scoped results. It verified clean teardown,
zero remaining project resources, and settled processes for each dispatched
fixture.

| Required node | Result | Observations |
| --- | --- | --- |
| `package/sharedTS18` | Passed | 24 checks across shared demand, global stop, and owner recovery. Two readers on two production replicas shared one upstream connection and one provider slot in each scenario. Six captures decoded audio and video. |
| `package/sharedTS6-all-down` | Passed | 14 checks. The durable input index was observed after both workers exited. Restart removed the index and released the provider slot without opening another upstream connection. Two captures decoded audio and video. |
| `package/retention100` | Passed | All 100 late readers matched a stored historical prefix across ten active channels without adding upstream connections. The late-read concurrency was 32. Twenty captures decoded audio and video. |

Four functional samples through 75 seconds observed at most 4,093,888 bytes
of local payload and 15,096,212 bytes of authoritative history payload per
channel. These were within the configured 4 MiB and 16 MiB caps. The receipt
covers these three fresh MPEG-TS workflows. Comparative performance and full
M9 acceptance remain open.

[Redacted scoped receipt](../scripts/rust-integration/milestone-batch113/cf64fbfe-functional-mpeg-ts.json)
