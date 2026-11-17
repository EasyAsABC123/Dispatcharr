# Source channel and guide coupling

A fresh pinned Source fixture completed twelve channel mutations and thirteen causal XMLTV readbacks. All four creates returned201, all eight override PATCHes returned200, and all thirteen XMLTV reads returned200. Six actual programme-import roots reached SUCCESS: five returned null and the orphan-guide root returned `EPG not found`. The literal controller exited0 in431.062 seconds with all357 inputs unchanged; independent cleanup verified three exact containers and two original process generations absent.

The cases cover dummy, XML and source-less guide channel creation; first, unchanged and changed dummy/XML override assignments; partial guide null clear; reassignment; entire override removal; and combined base-guide plus override creation. Full scoped SQL61tables/59sequences, raw wires, durable bounded journals, real task bodies/results, cache keys/generations, files and websocket observations remain private and hash-bound. The initial two Programme rows are declared setup, not programmes claimed to have been produced by HTTP.

Dummy-backed creation increments the output generation without publishing a programme job. XML creation and changed XML overrides increment once at the signal and once at successful programme completion. A source-less guide increments at the signal, publishes the real task, and returns its actual missing-EPG result. Unchanged guide override does not invalidate or publish. Clearing only its guide field invalidates without a job; combined base dummy plus XML override creation increments three times in total.

The original behavior has a specific difference when deleting the entire override row: it does not invalidate the output generation. The next XMLTV read serves byte-identical cached prior output even though the SQL override is absent. This observation is preserved as compatibility evidence; the migration must not silently substitute a cache fix. Retired cache entries otherwise expire naturally rather than being deleted wholesale.

These are positive owned admin mutation and network-gated XML observations. Wider roles, Schedules Direct assignments, fault/rollback branches, other output scopes and precommit scheduling interleavings remain unqualified. Actual Native parity and performance remain open. See [the compact evidence index](../scripts/rust-integration/channel-guide-source25/index.json).

All application, model, signal, task, protocol and algorithm credit remains with the original Dispatcharr and dependency authors.
