# Candidate9 recording continuation

Original Dispatcharr and dependency authors retain all application, algorithm,
protocol and license credit. React remains unchanged.

Runtime `3cd82adfb0c95bf30eb7cc537dceba21a0384464`, image
`sha256:946ebf97def2a3a3edf6019c622320a3c659e9e0f62db82ded8cf43db5cad860`,
was tested through the normal Compose application graph. The
[selected exact reports](../scripts/rust-milestone-batches/m478/batch9-recording-continuation/manifest.json)
retain the controller's exit1 after1042.819 seconds. They are a selected evidence
packet, not a portable closure of all fixture inputs or full acceptance.

Seven Native contexts passed: recording authorization, artwork, grouped scanner/
recurring/expiration/cleanup/statistics, overlapping playback including physical
slow-reader cancellation and graceful TERM, selective stop/delete/extension,
STREAMS network denial, and held-SQL shutdown. The held-SQL case intentionally
required exit1 without OOM and verified that an uncertain write was not replayed.
The slow-reader case observed an open, partially read file, actual descriptor
retirement after cancellation, stopped intake, and graceful Main exit0.

Core API requests returned200 but their expected tasks did not appear in the
configured broker database. Code inspection found that Core publication used the
cache connection rather than Main's shared Jobs connection. A later read-only
probe found both queues empty after the context reset; it cannot establish where
the earlier messages went. A shared-broker correction and a DB0/DB1 acceptance
check are prepared for the following candidate.

Native nonempty restart timed out. Retained observations show that named and
stale-clocked recordings completed, while recovery stopped after marking the
resume recording interrupted. Resumption and expiration acceptance remain open;
the failure does not qualify the full startup recovery workflow.

Source slow-reader setup failed because copying an existing rootless-owned media
file attempted an invalid UID/GID mapping. The corrected helper creates a fresh
controller-owned byte-identical staging file before copying; that correction has
not passed a new Source runtime test. Source restart timed out waiting for a
pidbox subscriber and remains failed. Previously qualified Source observations
were reused explicitly; the expensive Source Core scale workflow was not replayed.

The paired exceptional-range report also remains failed: retained Source
responses contained206 headers with negative content lengths, while Native
closed with no HTTP status or bytes. These literal observations do not qualify
the range error domain or justify inventing a416 response. Fixture and transport
semantics still require diagnosis.

All15 exact containers, both Compose project inventories and their recorded host
process generations were absent after cleanup. All recorded CLI/controller
groups settled with pipe EOF, and cleanup recorded no errors. The exclusive
functional lease was released. No benchmark or compilation ran alongside it.

Next, validate the shared Core broker correction and startup recovery repair
together with the pending normal backup, channel and Beat changes, build one
committed candidate, and run failed/dependent acceptance cells on that image.
No new CPU or memory improvement is accepted. M0–M9 remain open, including full
task/role coverage, SQLite compatibility, soak, restore, rollback and whole-graph
performance.
