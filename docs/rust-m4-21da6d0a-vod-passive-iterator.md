# Observed VOD iterator retirement on 21da

The paired scope retained the unchanged Rust package and instrumented only the disposable Original iterator and owned synthetic provider. At 32 and 128 KiB/s, each of the two shared Original readers closed while the iterator still awaited useful data. At each of the four closes a successful provider write overlapped the pending pull, and the iterator returned bytes before its close callback ran.

At 32 KiB/s, the observer bracketed the final Redis slot release between 900.06 and 1,100.19 ms after the final close for Original and within about 50.19 ms for Rust. At 128 KiB/s, both brackets ended at about 50.2 ms. Both rates completed the shared and independent reader checks and exact fixture cleanup. [Paired scope receipt](../scripts/rust-integration/m4-21da6d0a-vod-passive-iterator.json)

The trace covers provider writes and iterator pull and close boundaries. The uWSGI downstream write result and the early replacement request remain unverified in this scope. The run claims no comparative performance or full milestone pass. Original Dispatcharr and dependency authors and contributors retain full credit. [Original attribution](../ATTRIBUTION.md)
