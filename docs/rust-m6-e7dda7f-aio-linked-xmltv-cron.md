# Scheduled XMLTV ingest on e7dda7f

The current ordinary Rust AMD64 package ran with `DISPATCHARR_ENV=aio` and no scheduler ownership overrides. The new Compose run used the same workflow as the retained verified Original run. Before the schedule was due, the fixture observed an empty guide and zero requests to its owned upstream.

One natural UTC occurrence of the saved EPG source schedule made one upstream request. The job succeeded, produced the expected programme, and ran the linked Programme Index and Series Rules jobs with the expected lineage. The durable run count increased by one. Both child results settled and no messages remained ready or reserved. The run and independent comparison passed with exact cleanup.

The [machine-readable evidence](../scripts/rust-integration/m6-e7dda7f-aio-linked-xmltv-cron.json) binds the retained Original run, fresh Rust SQL and job records, and teardown evidence. This scope covers the linked daily XMLTV schedule in ordinary AIO mode.
