# One upstream for 100 simultaneous MPEG-TS viewers

The pinned Original and the production Rust `fdede54c` image each served
100 authenticated viewers on one channel in an isolated Compose run.
All 100 connections overlapped through the common five-second observation
window in each backend. Every viewer received new bytes and decoded both
audio and video. The synthetic origin reported one active connection,
unchanged cumulative connection count and advancing bytes across the
shared window. Its active count returned to zero after ordinary client close.
The same origin process stayed up across both backend phases. Its
cumulative counter was 1 during the Original phase and 2 during the later
Rust phase, counting the connections from both phases.

The controller and proof checker both exited 0. The comparison verified all
200 captured prefixes and decoder transcripts, all five owned role
generations, and empty container, network, and volume inventories.
The run evaluated functional fan-out. Comparative CPU and memory and full
M2 acceptance remain open.
[Receipt](../scripts/rust-integration/m2-fdede54c-functional100.json)

The original Dispatcharr authors and contributors retain full credit.
