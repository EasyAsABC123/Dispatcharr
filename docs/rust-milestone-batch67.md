# Candidate67 implementation and validation checkpoint

Original Dispatcharr and dependency authors retain all application, algorithm,
model and protocol credit. React and the original license are preserved.

Candidate source `a05b6745d651603097417686c18d0fcdad208f83` adds bounded
playlist-deletion previews and dummy EPG creation using the actual React payload,
including fractional duration and nested custom properties. It preserves Source
Unicode naming through the existing bounded dependency executor and corrects the
enabled-logo response Vary field. The runtime collector now distinguishes a
verified builtin unicodedata module from file-backed modules without relaxing
file-origin checks. Existing dummy detail and broader guide compatibility remain
outside this creation slice.

SDK115 passes formatting, workspace compilation, strict Clippy and tests:
**1,240 passed, zero failed, 802 ignored**, across 180 result blocks. All 1,468
bound inputs, including 1,462 Rust-tree files, remain unchanged. The final Docker
build completes with an observed exit 0 in 385.614 seconds. Its image is
`sha256:75af355bcb7f90304ac22a1db2a7ebe6b4da9549bbb5fc8d932257424e00ef8b`.
The earlier collector-origin build failure is retained. Build and cached SDK
timings are development measurements, not application performance gains.

All six [package families](../scripts/rust-integration/milestone-batch67/package/inventory.json)
pass with actual executing-agent waits and 55 exact containers independently
absent. The first sandbox-denied package attempt remains recorded separately.
Three [installed dependency lifecycle tests](../scripts/rust-integration/milestone-batch67/physical/report.json)
pass, covering Unicode behavior, normal shutdown, caller cancellation and
interrupted startup. Their controller and container cleanup are recorded; no
unrecorded wrapper process-group proof is inferred. These component checks do
not establish full Main application acceptance.

Ten new Source HTTP observations are captured and reviewed: six deletion-preview
requests and four dummy-create requests. Successful creation preserves all 24
React custom properties, including duration 180.5, sequence allocation and the
single causal WebSocket publication. Authentication, missing-object and invalid
input responses remain part of that reference. Source observations alone do not
prove Native parity.

The affected [ten-context Native batch](../scripts/rust-integration/milestone-batch67/native10-checkpoint.json)
completed with actual exit 1 in 1,103.821 seconds: **four passed, six failed**.
Guide coupling, enabled logos, live viewing and VOD adoption pass. Actual wait,
stream EOF, settled process group, unchanged inputs and independent absence of
11 containers and ten captured host generations verify.

Two new capture contexts fail before HTTP because the observer closure omits a
real helper import. Schedules Direct and range captures encounter a stale
observer scope check expecting 25 contexts instead of the selected ten. Those
captures require narrow offline qualification with the raw differences retained.
The first version task correctly clears its obsolete version notification and
preserves an unrelated startup warning; the observer wrongly compares the entire
notification table to its older reference. The remaining three version tasks
were not executed. A corrected observer passes the retained first-task capture
and seven negative controls, including unrelated-row mutation and wrong events.

The [three-context retry](../scripts/rust-integration/milestone-batch67/native3-checkpoint.json)
reused the image and the original backend captures. It exited 0 in 426.827
seconds. All four version tasks passed. The retry recorded six preview GETs
and four dummy-create POSTs, with six containers and five captured host
generations independently confirmed absent after cleanup.

The ten paired HTTP cases passed after individual review of all 42 literal
differences. All six preview response bodies matched byte for byte. The three
successful dummy responses matched after accounting for creation timestamps
bound to each request and its database row. The review retained differences
in wire headers and observer cache entries. It did not establish byte equality
for cache dumps or HTTP connection teardown.

The Schedules Direct and range observers qualified 23 retained calls against
the original backend with the scope correction. This leaves the first
controller's exit 1 intact. The combined evidence covers the ten selected
contexts and supports a bounded 100-viewer pilot. Resource gains require a
separate paired measurement under equivalent viewing work.

The [paired 100-viewer pilot](../scripts/rust-integration/milestone-batch67/viewing-pilot100.json)
exited 0 after 481.462 seconds including setup, both phases, media verification
and teardown. Both phases passed the fixed viewing window, drain, and complete
audio/video checks. Each used one upstream origin connection and one input
profile process. Both fixture projects were confirmed absent after cleanup.
Two earlier attempts exited 1 before viewer load due to Docker executable
resolution and guest read permissions. The corrected launch changed no
measurement payload bytes.

| Whole fixture, 100 viewers | Python | Rust |
| --- | ---: | ---: |
| CPU during the sampled window, core-seconds | 21.73 | 5.78 |
| Median charged memory, MiB | 2,135 | 1,896 |
| Median estimated working set, MiB | 1,206 | 638 |

The whole fixture includes backend, infrastructure, origin, and observer.
These are observations from one pair with idle background workers. The
[repeated live-viewing comparison](rust-candidate67-viewing-comparison.md)
subsequently passed all 12 scoped budgets at 1, 2, 10, and 100 viewers.

The [development plan](rust-development-and-validation-plan.md) uses three active
agent lanes plus integration, one coordinated Cargo target, one candidate image
and dependency-selected reruns. Real fixture concurrency remains disabled until
its isolation smoke passes. Performance measurement requires an exclusive quiet
window after functional acceptance.

**All complete M0–M9 milestones remain open.** No current application CPU or
memory gain is accepted. Remaining work includes
paired equal-work performance, unimplemented compatibility cells, soak and
rollback validation.
