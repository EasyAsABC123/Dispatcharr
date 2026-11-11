# Shared-buffer Source contracts

Original Dispatcharr authors and contributors retain full application and algorithm credit. AGPL licensing remains unchanged.

The pinned Original `StreamBuffer` now has 12 actual observation records using real Redis in an owned copied database. The fixture sets two TS packets per published chunk and a two-second TTL through the existing configuration. It captures split/truncated input, complete-packet flush on discontinuity, per-PID stamping, local reset, actual expiry, fresh writes after expiry and producer-stop behavior. All bytes are synthetic. No Source implementation is replaced with a Python in-memory ring or a no-Redis stub.

The effective default chunk size is `188 * 1361` bytes from `BaseConfig`, overriding the larger fallback literal in the buffer constructor. Source cursors are last-consumed; the prospective next-to-read adapter adds one. Fields describing this mathematical adapter are not native observations. Source `stop()` rejects writes while existing Redis data remains readable until expiry/cleanup; a future native comparison must account for that layer boundary.

[Source records](original-buffer-source-1.json) preserve full synthetic bytes and ordering. [Verified scope](verified-original-buffer-source-1.json) binds raw reports and the pinned archive. The initial mount-path setup failure remains recorded. The controller removes only its own project and namespace and audits the untouched default database.

```sh
source artifacts/rust/tooling/environment.sh
python3 scripts/rust-core-contracts/run_source.py --output artifacts/rust/core-source-NEW
```

M1 remains open. Next: native assembly/storage/runtime comparison of the same traces, held slow readers and final-owner accounting; then predeclare an equivalent paired shared-buffer resource matrix using common retention and successful full-byte work. Existing TS-stamping comparisons and Rust-only Arc fanout are separate evidence.

The later [mark-boundary component](verified-core-mark-components-1.json) adds a13th actual Original observation. After one incoming batch fills a stored chunk, a new PID in the pending remainder is already stamped in Original. The old native accumulator left it unmarked. Source mode now disarms stamping after the complete incoming frame, retaining the existing bounded one-chunk publication window. Exact captured bytes pass both the production assembly helper and a real local HTTP source-switch/flush test; strict workspace Clippy and369 default tests pass on630 exact inputs. The original12-record report remains historical evidence. No full native Redis/core comparison or performance gain is claimed.
