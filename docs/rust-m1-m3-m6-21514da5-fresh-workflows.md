# Shared media and selected worker checks on 21514da5

The exact Rust source passed the focused two-replica test against an isolated Redis container. A held response frame prevented that client from acquiring another frame while its peer continued to receive media. The dedicated test process and Redis container retired.

In paired HLS Compose runs, each backend served two readers through one upstream connection and one encoding process. The viewers matched the regular segments by sequence, both decoded audio and video, and the owned processes retired. Fragmented MP4 also used one upstream per backend. Both viewers decoded six shared complete fragments using one shared encoder and one remux process. The remaining viewer progressed after its peer closed.

The paired playlist import completed both account jobs from the ordinary aggregate request. The Rust arm ran Selected Rust Jobs with Beat disabled. It imported one two-member gzip playlist and one plain playlist, with matching stream and group effects and real terminal worker results. Both projects verified exact teardown.

Full M1, M3, and M6 acceptance and the current comparative performance campaign remain open. [Exact package and redacted results](../scripts/rust-integration/m1-m3-m6-21514da5-fresh-workflows.json)

Original Dispatcharr, FFmpeg, and dependency authors and contributors retain full credit. [Attribution](../ATTRIBUTION.md)
