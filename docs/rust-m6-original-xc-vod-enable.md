# M6 stock XC VOD enable publication

The pinned Original Dispatcharr image handled an authenticated settings PATCH for an existing active XC account. Changing `enable_vod` from false to true added one actual ready root for `apps.vod.tasks.refresh_vod_content` on the default `celery` queue. The queue had no VOD refresh beforehand. The fixture compared ready messages byte for byte by queue and verified that all existing entries were preserved. The new message identified the fixture account and carried no parent. The isolated fixture launched no worker or Beat and qualified no task execution or result.

The two GETs bracketing the PATCH and the PATCH itself each returned HTTP 200 with JSON. PostgreSQL showed the new VOD flag, the preserved other request settings and schedule identity, and two `Uncategorized` links for movie and series, both disabled under the seeded VOD group preference. The separate provider and profile state stayed the same. The exact stock web and owned origin generations retired, and the owned container, volume, and network inventories were empty.

Original source revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and image `sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f`. The [redacted receipt](../scripts/rust-job-contracts/verified-original-xc-vod-enable-1.json) has SHA-256 `10d773cb6dd48992354852d3aa325567c3816105c575b3544f89c877d65689a5`.

The first run exited 1 during the settings capture. The second stopped before the PATCH because its empty-queue assertion failed. All owned roles retired after both attempts. The fresh run used bounded queue snapshots to distinguish pre-existing messages from the new VOD root.

The [earlier direct-refresh worker receipt](../scripts/rust-integration/vod-ingestion-source58/qualification.json) covers a separate Original request and successful worker run. For the new settings-triggered flow, Native producer parity, worker completion, and M6 acceptance remain open. Performance was outside this fixture.

The original Dispatcharr and Celery authors retain full credit.
