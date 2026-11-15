# Combined auto-sync and stream-health candidate

All original Dispatcharr, Python, Unicode and dependency authors retain full
application, algorithm, protocol and license credit. React is preserved.

The joined candidate passes formatting, workspace compilation, strict Clippy
and the full workspace suite: 1,023 passed, zero failed, 758 ignored, across
174 result blocks. Its [SDK evidence](../scripts/rust-integration/milestone-batch19/inventory.json)
binds 1,292 selected inputs, including 1,286 Rust files. The preceding integration
compile and lint failures remain recorded. Ignored tests and unexecuted Docker
cases do not count as acceptance.

This candidate joins the real normal-worker and inline import paths for channel
auto-sync, bounded SQL-backed channel deletion, full child/result metadata
spooling, and pinned Source Unicode/regex behavior. It retains the existing SQL,
Jobs and task admission budgets. The lazy CPython/regex dependency process runs
the pinned regex algorithm; Rust owns backend orchestration and effects. This
does not enable the broader Python plugin host or replay Source backend tasks.

Remote stream viewers now use Source's raw last-data timestamp and two-second
health cache after the required empty-read/head gate. Actual accepted ingress,
including partial chunks, refreshes the owner-fenced timestamp; cleanup drains
ingress before clearing it and releasing ownership. Source's healthy local-owner
behavior and keepalive bytes remain unchanged. Actual Redis peers and remote
application behavior still require their declared runtime checks.

Main now supplies the existing adaptive Beat scheduler Source's five-second
maximum sleep, replacing the accidental 100 ms cap. Earlier due entries and
immediate due backlog retain their existing shorter delays. The separate queue
polling cadence is unchanged. The pinned Source deployment uses that five-second
default; functional fixtures with explicit overrides are separate authority.
This correction addresses a concrete suspected cause of the higher CPU use in
the [candidate18 integration capture](rust-milestone-batch18.md), and is not yet
a measured gain.

The next Docker image must also include the already committed VLC plugin-path
launcher correction. Combined Compose acceptance covers actual API import,
channel/profile/EPG effects, real large task fanout, regex semantics and process
ownership, VLC media, stream controls and teardown. The full fixed resource
workload follows functional acceptance with all participating processes counted.

Compact numbering, the new SD guide consumer, broader account CRUD/signals,
Connect/plugin hooks, enabled plugins, active output profiles and other source
edge/fault domains remain explicit open work. Full-reader API acceptance and
old-SUCCESS/result-subscription semantics require their actual entry points;
direct Redis observations alone cannot qualify them. No full M0–M9 milestone or
new application performance gain is accepted.
