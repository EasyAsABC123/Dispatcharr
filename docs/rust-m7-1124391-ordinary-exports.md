# Ordinary M3U and XMLTV exports on 1124391

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

The verified ordinary image passed 15 paired M3U GET cases and four method representations for each backend. A separate paired XMLTV run passed 13 GET and HEAD cases per backend in the declared clock window. Both comparisons used the same exact Rust image and each verified its own Compose teardown. These results cover the export requests defined by the retained workload. Realtime, broader protocol parity, and output resource comparisons have separate gates. [Machine-readable evidence](../scripts/rust-integration/m7-1124391-ordinary-exports.json)
