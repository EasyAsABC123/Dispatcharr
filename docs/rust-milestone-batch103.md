# XC playback, recurring DVR purges, and scheduled guide work

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit. The exact Docker build preserves all 497
React, license and attribution inputs from Candidate100.

The verified source revision is `e4317c4bbcfc427943c1eaf19e0c6f7d43d05a0c`.
The first revision failed compilation on a missing HTTP header import.
The corrected snapshot passed formatting, strict Clippy, and 935 focused
Rust tests, with 222 external-fixture tests ignored.

The XC Compose pair matched all 13 Original and Native responses, including
redirects, explicit range requests, and authorization for each follow.
The separate UUID proxy now matches the Original HTTP 403 response bytes.
The owned Compose project was removed.

Three recurring DVR changes matched the Original. PATCH and PUT can disable
an enabled rule, and DELETE can remove an enabled rule. The fixture checks
future recordings, Beat rows, and preserved unrelated data. It also captured
the Original's two synchronous enabled rebuild cases for the next candidate.
Both projects verified owned cleanup.

The first scheduled XMLTV invocation exited 1 during argument validation
because it omitted the local Original image ID. Both repaired invocations
then stopped at the startup barrier. The active XMLTV seed triggered an
immediate refresh whose two child jobs entered the startup observer
alongside the three expected startup tasks. Both lanes verified cleanup
and recorded zero scheduled business cases. The next fixture uses an
inactive seed, then enables and arms its existing schedule after startup.

Both ordinary five-minute password cases and the controlled Redis
disconnect have passed. The [auth diagnostic report](rust-m9-playback-refresh-diagnostic.md)
records their exact scope. These targeted checks leave the full milestone
acceptance and performance gates open.

[Scheduled XMLTV setup evidence](../scripts/rust-integration/milestone-batch103/scheduled-guide-first-pair.json)

[Functional receipts](../scripts/rust-integration/milestone-batch103/functional.json) ·
[Preceding XC comparison](rust-milestone-batch101.md)
