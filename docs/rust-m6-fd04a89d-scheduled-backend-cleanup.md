# Scheduled Redis backend cleanup on fd04a89d

Original's stock worker and DatabaseScheduler and the packaged Rust
Selected worker and Beat each ran one due cleanup at the 04:00 UTC
schedule. Both recorded one durable run, a successful stored null
result and empty queues. The configured expiry was 43,200 seconds.
The existing Version result kept its exact bytes and aged normally.
Other schedule rows matched the pre-run state.

The two startup recovery and Version workflows settled on each
backend. The fixture observed one Version origin request and one
Version event per arm. The worker and role cleanup passed on both
backends, with six Original and five Native role generations retired.

This covers the ordinary selected-mode schedule with the fixture's
startup plugin-discovery opt-out. Full M6, default deployment behavior
and performance remain open.
[Redacted scheduled cleanup receipt](../scripts/rust-integration/m6-fd04a89d-scheduled-backend-cleanup.json)

The original Dispatcharr, Django, Celery and dependency contributors retain full credit.
[Attribution](../ATTRIBUTION.md)
