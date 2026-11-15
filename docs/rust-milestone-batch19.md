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

The first image build failed after 110.35 seconds because the dependency
collector requested `/app/LICENSE`, which is absent in the pinned Source image.
The final Docker stage already retains the repository license and attribution.
The packaging-only correction removes that duplicate lookup and its obsolete
empty application-prefix iteration. A direct network-disabled Source audit now
passes in 3.28 seconds, retaining 51 ELF records and 626 prefix files, with actual
CPython 3.13.18 and regex 2026.9.29 notices. The initial direct audit also caught
an obsolete variable reference; that failure is retained. Both audit containers
are absent. No Source backend was started.
The [packaging correction](../scripts/rust-integration/milestone-batch19/packaging-correction.json)
and separate build-input map preserve the exact SDK snapshot while accounting
for the collector change. Rust language sources are unchanged. A corrected
image and its actual dependency/application checks remain required.

An additional direct audit executes the actual isolated Python CLI and imports
the pinned regex package, confirming its relocated prefix and absence of Source
Django. It passes in 3.19 seconds. The preceding temporary fixture denied exec
on its output mount; that failed attempt is retained. The CLI argument-quoting
correction belongs to the packaging-only change.

The normal Main lifecycle still needs active auto-flow cancellation: an
unbounded search can prevent the worker from reaching its existing regex
retirement call. Component child-retirement capability does not establish that
Main drives it before waiting for tasks. The corrective shared drain hook is
separate open work; ordinary regex acceptance will not qualify a held-search
shutdown from a passing normal capture.

The corrected second image builds and passes archive/UID/entrypoint,
React/credit, GPG, PostgreSQL and paired VLC useful-core checks. The complete
1,783-file archive matches its Git revision. VLC's selected core contains the
original 75 video and 142 AAC frames, with raw finite-tail limitations retained.
The package test's first import attempt mishandled literal VLC template braces;
the corrected observer substitutes only Source's named placeholders.

The final-image regex check then fails with `ModuleNotFoundError: regex` despite
readable, byte-exact retained files. Actual interpreter queries show why: Source
runs in a venv using `site-packages`, whereas the relocated final interpreter
discovers Debian `dist-packages`. The earlier Source-only CLI check did not
certify module origins and was insufficient. The collector now installs the
same bytes under the recognized directory and audits every loaded regex module,
including its extension, and every search path under the copied prefix. That
direct audit passes in 3.20 seconds. Explicit public bridge copy modes also
support direct workspace builds; archived image19's bridge was already readable.
The [site correction](../scripts/rust-integration/milestone-batch19/module-site-correction.json)
awaits a new image and actual nonroot runtime/normal-Main checks.
