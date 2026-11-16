# Source Series ingestion and nonempty Comskip characterization

Original Dispatcharr and dependency authors retain all application, algorithm,
protocol and library credit. React and licensing remain unchanged.

One existing Compose bootstrap ran two ordered Source contexts with independent
database/cache/media/worker boundaries. The [actual controller](../scripts/rust-integration/series-comskip-source1/execution.json)
exited **1 in143.381 seconds**, with both pipe EOFs and its process group settled.
Five of six cases passed. This whole-run failure remains literal; no Source
workflow was replayed or acceptance threshold changed.

The [Series context](../scripts/rust-integration/series-comskip-source1/Series/report.json)
passes all four natural HTTP refresh phases: fresh two-Series catalogue with no
Movies; unchanged repeat; relation reassignment/global identity deduplication
with category replacement; and empty-Series stale cleanup. Every phase observes
five real TLS provider requests, one actual claimed root with backend SUCCESS
and no children, SQL and two ordered WebSocket updates, and the ordinary Series
list endpoint. The decorated process_series_batch helper runs inline, without
an invented queue worker or automatic episode fetch.

The actual plain domain has year NULL even when the title contains2020; Series
year comes from release dates. Source retains category_id in Series properties.
Repeats preserve identity/timestamps; reassignment resets both detailed_fetched
and episodes_fetched while preserving unrelated properties and
last_episode_refresh. Two provider IDs can share one global Series. Category
replacement and empty-Series cleanup produce the retained expected SQL states.
An additional offline check confirms the surviving global Series ID/UUID and
creation/update timestamps stay stable through category replacement. The
[qualified Series leaf](../scripts/rust-integration/series-comskip-source1/Series-qualification.json)
and [semantic projection](../scripts/rust-integration/series-comskip-source1/Series-semantic-projection.json)
provide Native comparison authority, separate from the failed whole cohort.
General metadata, release dates/languages/external IDs, Episodes, roles and full
API serialization remain outside this scope.

The [nonempty Comskip target](../scripts/rust-integration/series-comskip-source1/Comskip-nonempty/report.json)
failed. Actual unmodified Source processed137-second synthetic A/V with an owned
supported custom INI and returned backend SUCCESS/no_commercials, completed/
skipped properties and an empty EDL. The subsequent ordinary request passed
already_processed. Input hashes stayed unchanged. Backend/SQL/ordered WebSocket,
media and diagnostic sidecar observations were saved before the failed target
assertion. The [actual detector log](../scripts/rust-integration/series-comskip-source1/Comskip-nonempty/actual-detector.log)
reports3425 processed frames, one136.92-second block, zero strict black-frame
cuts and zero commercial duration. This does not isolate why the planned black
boundaries produced no accepted break. No Rust defect is inferred.

The independent input observer really verified A/V and duration with FFprobe;
the failed target does **not** qualify Source's later task FFprobe/nonempty-mark
branch, automatic DVR publication or cut mode. No EDL was prewritten, no tool
was replaced and no result was manufactured. The existing eight-second
no-commercial reference and no-file case were retained without replay.

Root independently verified both exact fixture containers, their two original
positive host process IDs, all project inventories and the UID2 probe container
were absent. UID1 failed host preflight before Docker because an offline import
had created a derived mode0700 Python cache. Only that owned bytecode cache was
removed; all166 frozen payload hashes stayed unchanged and fresh UID2 passed.
Both attempts remain in the [evidence index](../scripts/rust-integration/series-comskip-source1/index.json).
No Native, full milestone or application performance acceptance is claimed.
