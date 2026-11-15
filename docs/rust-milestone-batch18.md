# Candidate18 healthy-owner keepalive correction

Original Dispatcharr and dependency authors retain full application, algorithm
and license credit. React is preserved.

The local Source-owner transport now sends keepalive packets only at the buffer
head, after five empty reads and while its actual manager is unhealthy. Empty
reads use Source's backoff, and the keepalive duration cap starts with the first
keepalive and resets after media recovery. Pending history reads retain their
existing ownership; a small cursor observation follows historical repositioning.
The existing Source packet bytes are unchanged.

The real HTTP regression holds a healthy input between media chunks and checks
that the response waits for media rather than emitting a null packet. It fails
against the old engine with exit101 and passes with this correction. Formatting,
compilation, strict Clippy and the full workspace pass:1,008 tests, zero failures,
756 ignored tests and173 result blocks. The
[SDK evidence](../scripts/rust-integration/milestone-batch18/inventory.json)
binds1,261 selected inputs, including1,255 Rust files. The first failed compiler
attempt remains retained locally.

This is the local-owner correction. Remote-owner health requires Source's real
last-data timestamp, two-second health cache, dynamic timeout and retained Redis
read/write/cleanup ports; that separate implementation is prepared for integration.
No remote-owner acceptance is implied by the local HTTP test.

The Docker build now passes at `8cf704d2794ec3230b3c2856fd66dffe2d01720d`,
producing image
`sha256:7ede9938178cfdfae4f447fcdfce4ff5fd9f072596188d954710e52f09776f74`.
All 1,752 archive files match that Git revision: 1,255 Rust, 495 React and two
credit files. The physical image runs as UID 65532; original React assets and
the original attribution prefix are preserved. The packaged GPG and PostgreSQL
tools pass their version and hash checks (27 and 528 retained files).
The [package evidence](../scripts/rust-integration/milestone-batch18/package/)
records the actual checks and their limited scope.

VLC packaging remains an open gate. Both attempts verified its installed closure
and the three custom ELF-to-original-notice links. The first observer incorrectly
expected a different version prefix; its failure is preserved. The second
attempt reached remuxing: candidate VLC exited 1 with no output, while Source
exited 0 but its output produced an AAC decode warning. Explicit writable HOME
did not fix the candidate. Module discovery isolated the packaging defect:
relocated libvlccore found only its core module until the retained original plugin
directory was supplied, after which it loaded 466 modules. The collector launcher
now supplies that directory and preserves caller additions; this correction
still requires a new image and remux acceptance. Source's warning and actual
decoded frame counts remain recorded; no media assertion has been waived.

The corrected common-supervisor integration now passes both complete loaded
idle-consumer live-viewing phases: Source exit 0 in 145.14 seconds and Native
exit 0 in 134.98 seconds. Both record 61 samples across the fixed 60-second
window and pass the existing complete-media certificate, including clean
certified A/V decode. Each graph has the same PID 1 fixture supervisor, six
slots and twelve drain workers; Source has no extra Python role controllers.
All six Source roles stop normally with wait/EOF receipts; Native Main exits 0
without OOM. Independent teardown checks find all seven container IDs and
73 recorded host PIDs absent, both Compose projects empty and observer scopes
empty or absent. The
[integration evidence](../scripts/rust-integration/milestone-batch18/common-supervisor/)
accepts this selected application/media workflow, not the full 24-phase resource
workload or a performance gain.

The two-phase resource observations remain descriptive. Source's primary mean
CPU is 0.06353 cores and Native's is 0.08737 cores (ratio 1.3753); primary peak
working-set estimates are 920,453,120 and 246,501,376 bytes. Native Main accounts
for only 0.01116 cores; its shared infrastructure container accounts for
0.07334 cores. The higher CPU use requires investigation before the full fixed
paired workload. No memory percentage is promoted as an accepted application
gain from this integration sample.

Wider combined application acceptance remains required. The
[preceding resource capture](rust-batch16-resource-observations.md) remains failed
and descriptive: extra Source fixture controllers biased accounting, and the
old Native transport emitted keepalives during healthy input. The replacement
common controller has passed a real six-child lifecycle peer; its complete
application workflow and fair paired measurements remain pending. No full
M0–M9 milestone or new performance gain is accepted.
