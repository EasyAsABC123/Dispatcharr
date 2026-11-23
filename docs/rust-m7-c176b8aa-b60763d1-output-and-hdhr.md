# Scoped output and HDHomeRun comparisons

The ordinary c176b8aa AMD64 image matched 39 output and metadata reads
per backend, three signed assignment mutations, and 20 advertised
guide links. The read-only metadata cohort matched body digests and
selected wire headers, and the scoped SQL and teardown checks passed.

The ordinary b60763d1 ARM64 image matched configured HDHomeRun discovery
and playback for one public device/profile case. Three metadata
requests and one delivery request per backend reached the intended
advertised URLs. Audio and video decoded and exact teardown passed.

Broader M7 acceptance remains open. Original Dispatcharr, FFmpeg
and the upstream contributors retain full application and component
credit.

[Machine-readable qualification](../scripts/rust-integration/m7-c176b8aa-b60763d1-output-and-hdhr.json)
