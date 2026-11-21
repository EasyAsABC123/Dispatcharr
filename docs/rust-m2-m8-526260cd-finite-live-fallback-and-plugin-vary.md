# Live MPEG-TS fallback and trusted plugin headers on 526260cd

The Original and Rust MPEG-TS runs each kept two authenticated viewers
playing through three clean EOFs at the primary URL and one distinct fallback.
The Origin saw four requests and a maximum of one active upstream. Both
viewers decoded audio and video at the primary and fallback. The active
stream metadata identified the fallback on each backend. Original's legacy
reservation alias stayed on the primary. Rust's legacy alias changed to the fallback.
Both runs verified retirement and exact owned cleanup.

The first Original controller exited 1 after expecting the legacy reservation
alias to change with the active stream metadata. Its exact cleanup passed.
A corrected fixture checked the two fields independently and passed. The
earlier literal exit and corrected Original receipt are retained.

The first Rust run on this image exited 1 with exact cleanup after both
viewers received three full primary payloads. Its retained capture contains
no fallback census. That fixture marked its origin handler retired after a
pacing wait that began after it had already sent the last declared body
bytes. The corrected observer settles the final successful write under its
observer lock, records the census
before checking it, and reruns fresh Original and Rust on the same workload.
Both corrected runs verify ordered body settlement before the distinct
fallback, exact cleanup and useful playback.

The same Rust image ran five successful trusted plugin steps in Selected
compatibility mode. It enabled and listed the plugin, invoked its echo
action once, disabled it, then listed it again. Selected HTTP headers,
including `Vary: Accept, origin`, matched the four retained Original TCP
cases. The fixture verified SQL state, action ownership, Main exit, exact
Compose cleanup and compatibility-process retirement.

The exact workspace passed Rust 1.90.0 formatting, strict Clippy, and
1,511 enabled Cargo tests. The React files and
original notices were unchanged in the verified package.
[Redacted paired and Selected receipts](../scripts/rust-integration/m2-m8-526260cd-finite-live-fallback-and-plugin-vary.json)

M2 and M8 remain open. This evidence covers finite-EOF live fallback and one
trusted Selected plugin lifecycle. Full playback and resource acceptance,
default plugin ownership and broader plugin compatibility require their
existing gates. Original Dispatcharr, Django, FFmpeg and dependency authors
retain full credit. [Original attribution](../ATTRIBUTION.md)
