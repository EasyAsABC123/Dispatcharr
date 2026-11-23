# Pinned Original output hash and guide grammar

The stock Original passed 24 signed XC reads across hash seeds 1 and
4294967295 and two named XMLTV reads. All 136 matched generated
programme IDs changed between seeds. Stored programme IDs and response
bytes were identical across both process generations.

The two final timed guide rows were checked against each request's
recorded clock and Original's three-day cutoff. An earlier comparison
failed because it required these clock-derived timestamps to be equal.
The fresh run verified the non-UTC named capture and matching cold and
warm XMLTV bytes, one actual guide download event, unchanged scoped
persistent SQL and exact retirement.

These results establish the selected Original behavior for the Rust
comparison. Wider realtime and output coverage remains open. Original
Dispatcharr and its upstream contributors retain full credit.

[Machine-readable qualification](../scripts/rust-integration/m7-original-output-hash-grammar.json)
