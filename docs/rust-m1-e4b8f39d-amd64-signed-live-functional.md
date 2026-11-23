# AMD64 signed live TS at 1, 10 and 100 viewers

The packaged Original and Rust each completed three signed live MPEG-TS cases with 1, 10 and 100 viewers. All 222 continuous HTTP requests returned 200, delivered at least the 2 MiB checked prefix and passed the real decoder. The runner observed one concurrent upstream connection while the viewers watched in each case. The independent owner also verified zero active upstream connections after each drain and retired all nine owned container identities and the remaining project resources.

The six functional phases use five seconds each for warmup and observation, with explicit decode and shutdown gates. The separate quiet CPU, memory, delivered-byte and first-byte performance comparison remains open. See the [sanitized functional evidence](../scripts/rust-integration/m1-e4b8f39d-amd64-signed-live-functional.json) for the exact candidates and phase counts.

Original Dispatcharr authors and contributors retain full project credit.
