# Candidate17 plugin, playlist and input-profile implementation

Full application and algorithm credit belongs to the original Dispatcharr
authors. VideoLAN/VLC and all dependency authors retain their credit and license
notices. React is preserved. VLC packaging adds explicit attribution without
removing any earlier attribution bytes.

The combined candidate passes formatting, workspace compilation, strict Clippy
and 1,004 tests, with zero failures and 756 ignored tests across 173 result
blocks. The [SDK packet](../scripts/rust-integration/milestone-batch17/inventory.json)
binds 1,260 selected inputs: 1,254 Rust files, attribution and five coupled
fixtures. Earlier compiler and lint failures remain recorded. All work uses
one coordinated Cargo target; no compiler cache was copied into this packet.

Plugin actions now wire disabled-plugin discovery, manual ZIP imports, settings,
logos, deletion and managed repository installation/update through Main. Managed
downloads use the existing HTTPS, redirect, DNS, SSRF and NO_PROXY transport,
owned file writes, conditional SHA256 and retained-key archive replacement.
Catalogue/detail handling retains GPG verification and shared cache ownership.
Repository settings reuse the admitted SQL session for the existing Beat setup
algorithm. Auth writer limits, the existing SQL pool and shutdown ownership are
shared. Enabled Python execution remains a required compatibility boundary.

Remote STD playlist handling joins the normal worker's existing M3U admission,
locks, SQL session, cache parser, import/status/events and result transport.
Downloads and full Groups results use owned files and bounded chunks rather
than allocating the entire result tree. Redis staging retains the complete
result with an atomic publication boundary; its physical peer and large-result
consumer coverage are still required. Generic backend readers, Source import
concurrency, auto-sync, hooks and account/profile/filter signals remain open.

Input-profile work adds Source-derived VLC error-marker handling and a pinned
VLC binary/plugin/data/license packaging collector. Grouped XC reservations now
carry current credential identities through primary and failover selection.
The selected domain covers ASCII username normalization and limited XC groups;
STD sample-URL precedence and non-ASCII username behavior remain unqualified.
VLC execution, grouped capacity/release/failover, Redirect and output-profile
delivery require actual paired application acceptance.

This candidate has code-level validation only. Its Docker build, runtime package
probes, combined Source/Native application acceptance and representative paired
performance are pending. Candidate16 functional evidence and performance runs
keep their own image identity and cannot be transferred to17. Full M0–M9,
SQLite compatibility, complete source coverage, enabled plugin behavior,
soak and restore/rollback acceptance remain open.
