# Paired aggregate and individual XMLTV refresh

One isolated Compose batch used Rust application `d482e39c5196f3044e91321f58766f1b134f8d25`, Native image `sha256:049eb37f5f898c09d2fd694d13c7e3be99d9ae0e9354bd4ef34a0f7585bb4fea` and Original image `sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f`. Original Dispatcharr, Celery and Django authors retain full credit.

A custom Beat entry ran once on each backend. Two equal-payload XMLTV sources were refreshed inline in the observed alpha-to-beta order. Each produced one programme and an index. An inactive source and an active dummy stayed idle with no programmes or indexes. The fixture's observed order does not establish an application-wide sort.

Both aggregate roots had empty positional and named arguments and null time limits. Each completed with the exact string `EPG data refreshed.` before its four children finished. Index(alpha), Series(), Index(beta) and Series() were published and completed in that order, each with the aggregate as both root and parent. Both roots listed the four child result tuples in publication order. The Index results were null and both Series results recorded zero scheduled work.

The same packaged Native image also matched the unchanged individual scheduled XMLTV case. Its root retained `[14400,null]`, returned null and recorded two settled descendants. Each backend wrote a single programme and a populated index.

The combined controller and four lanes exited 0. Both Native Main processes exited 0. Exact cleanup found no resources or ready and reserved work. The verified package manifest SHA-256 is `9fc786b8fdf70775bf4bf9a7239e4e13f9463d00f3d55cff33ca84e3f7c21833`. The redacted receipt SHA-256 is `297238a5028bc133103e43cb02f1bd4d5b4729ac7a86dc70f026f9b0a2d87fd9`. Broader source classes, plugin/Connect configuration, full M6 and performance remain open.

[Redacted paired XMLTV receipt](../scripts/rust-integration/milestone-batch113/d482-aggregate-and-individual-xmltv.json)
