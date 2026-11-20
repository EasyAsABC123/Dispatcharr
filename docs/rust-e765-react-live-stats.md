# e765 React live playback and Stats

Pinned Original and exact Rust e7651a6d used the same 112 shipped React
assets. Both real Chromium sessions signed in through the normal form,
previewed a live channel and decoded advancing MPEG-TS video. Both navigated
to Stats and received HTTP 200 with the active channel in the live section,
then closed the player with the normal UI control.

The scoped owner, evaluator and cleanup each exited zero. Independent checks
confirmed that all four Compose roles, the frontend asset donor and both
Chromium groups were gone, with empty container, network and volume
inventories. The wider coordinator exited 2 because this was a scoped node
run. The first exec passed both browser arms and independently verified
container and donor retirement. Its immediate Chromium retirement check
failed, so the first cleanup and evaluation each exited 1. A later
read-only check found both groups gone. The fresh retry reaped only the
adopted children of the two exact browser groups before verifying absence.
Full migration acceptance and performance remain open.

Original Dispatcharr authors and contributors retain full application and
interface credit. [Redacted browser receipt](../scripts/rust-integration/milestone-batch113/react-live-stats-e765.json)
