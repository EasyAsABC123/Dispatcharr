# Bulk channel visibility and compact numbers

The exact `11ddf3e2` package matched two authenticated bulk edits per
backend in each of the noncompact and compact channel workflows. Rust ran
through Main with the default Core configuration. Selected Jobs and Beat
stayed off. The two comparisons started with the same scoped PostgreSQL
state and verified that four owned roles retired in each fixture.

The noncompact comparison hid and unhid two selected managed and manual
channels. Their numbers, override clock and pinned values stayed intact.
The compact comparison assigned the newly visible channel number 3002
while 3000 was occupied, then released the hidden channel's number. The
inverse restored the original rows. Original's PATCH returned the
pre-assignment value for the newly visible channel. Its detail GET showed
the assigned number. Rust matched both observations. The manual channel
kept its number and the unselected control kept its reservation.

The first stock fixture expected the assigned number in the PATCH response.
It exited 1 and verified cleanup. The corrected fixture and the fresh
pair passed. The detail GET `Allow` header remains a tracked wire
difference. Broader M5 and performance gates remain open. The original
Dispatcharr authors and contributors retain full credit.
[Redacted receipt](../scripts/rust-integration/m5-bulk-visibility-11ddf3e2.json)
