# M6 stock XC provider deactivation

Pinned Original returned HTTP 200 JSON for an administrator's initial
read, the React table's single-field PATCH, and readback. The owned,
active XC provider became disabled. Its existing VOD preference, two
category links and movie relation remained intact. The other provider,
profiles, streams, channels and linked data were unchanged.

The linked four-hour task retained its identity, interval and run count.
It became disabled, cleared its last-run time, and advanced the task and
Beat update timestamps. Original deleted two owned expiration notices and
their two user dismissal records. The Channels updates group delivered
the two matching dismissal events. The subsequent read changed no SQL
rows. The stock web and prefork worker ran with Beat off. No new broker
items or result stores were observed. Both owned roles and their
container generations retired and the project inventories were empty.

The redacted stock receipt SHA-256 is `917e2bf7d4e41146cc90d99c4be1db25bcad5ff80296e52c5f6caa9ff591c964`.
The current Rust port and broader M6 acceptance require fresh paired
execution. The fixture did not measure performance. [Redacted stock receipt](../scripts/rust-job-contracts/verified-original-xc-deactivation-1.json)

The original Dispatcharr, Django, Celery, Django Celery Beat, and Channels
contributors retain full application and protocol credit. [Attribution](../ATTRIBUTION.md)
