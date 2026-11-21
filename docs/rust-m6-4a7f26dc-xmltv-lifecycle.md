# XMLTV creation and guide deletion on 4a7f26dc

Three normal XMLTV creates returned HTTP 201 on each packaged backend. Both active sources ran one initial Refresh and completed Index and Series. The zero-hour source kept a linked disabled hourly row, the six-hour source kept an enabled six-hour row, and the inactive twelve-hour source kept a disabled twelve-hour row and produced no job. Each backend made two owned provider fetches. Native used the explicitly selected worker. The complete default queue and scheduler remain open.

An authenticated delete of an XMLTV source in use returned 204. Each backend cleared two channel links and two override links, deleted the owned guide and schedule data, and retained the independent source and valid override fallback. The following read returned the typed 404. Both workflow receipts verified actual Compose cleanup. [Exact package and paired receipts](../scripts/rust-integration/m6-4a7f26dc-xmltv-lifecycle.json)

The first interval reducer exited 1 after both physical runs passed. It compared the Original archive digest with a per-file map. The offline retry verified the archive digest and the complete extracted file map independently, then qualified both retained results without rerunning the fixtures.

The frozen Rust workspace passed formatting, strict Clippy and 1,498 enabled tests. Comparative performance remains open. Original Dispatcharr, Django, PostgreSQL and dependency contributors retain full credit. [Original attribution](../ATTRIBUTION.md)
