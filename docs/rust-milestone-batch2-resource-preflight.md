# Shared-input resource preflight: unqualified

The balanced Original/Rust/Rust/Original diagnostic preflight stopped after its
first actual Original phase. No Rust measurement phase ran and no ratios or
CPU/memory gains are accepted.

The30-second window retained31 raw cgroup samples, HTTP200 readers, zero sync
errors, and one active upstream/provider with two clients. Measured byte advances
were4,605,624 and4,861,492. They failed the frozen5,000,000-byte-per-viewer gate;
13 sample ticks also had a viewer with no observed byte advance. Later phases
and the planned off-window decoder were not run, as the frozen protocol required.

Offline diagnosis found that the2Mbps origin includes mux padding, while the
copied FFmpeg profile remuxes without a fixed output muxrate. Origin bytes need
not equal useful output bytes. Source's observed255,868-byte batching also makes
per-second progress an inappropriate continuous-usefulness assumption. Separate
offline packet analysis found roughly29–31 seconds of video PTS in the retained
window. That diagnosis does not retroactively pass the failed gates or create a
missing decoder result.

The [frozen workload, executed tools, summary and diagnosis](../scripts/rust-milestone-batches/m235/resource-preflight/manifest.json)
retain the failure. Whole-fixture accounting included every owned service,
deterministic origin and dedicated observer scopes. Eight fresh cleanup checks
passed, including empty project inventories, absent actual CIDs and retired
systemd scopes.

A future separately frozen protocol must derive useful-work tolerances from the
fixed media, PTS/content and actual buffering cadence. It must not lower gates
after observing a result. Full M3 and whole-application performance remain open.
Original Dispatcharr and dependency authors retain full credit and licenses.
