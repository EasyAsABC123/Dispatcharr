# Notification controller prepared for Root review — never executed

This private draft prepares the eight Source-only requests in the earlier proposal. It has no registration, application result, new assignment, Native result or endpoint acceptance. Original Dispatcharr authors and contributors retain application and algorithm credit and licensing. The three original Factory tests retain their wrong `/api/system/notifications/` strings byte for byte; this draft never sends those paths.

`controller/run.py` checks local sealed bytes by default. Source execution requires Root's explicit review of the exact `seal.json` SHA256 and a new output directory. The pinned image already exists; execution has `pull_policy: never`, `--pull never`, no build, no published ports and one internal network. The controller unpacks the exact historical Original archive, mounts it read-only and overrides only the container entrypoint. Unmodified Original settings, URLconf, permissions, serializers, models and initialization guards run in management `migrate` and `shell` modes.

The Source-only bootstrap starts local PostgreSQL and Redis, runs migrations, then idles. It starts no web supervisor, worker, decoder, provider fixture or sampler. This deliberately refines the proposal's older playback-supervisor startup: there is no web/worker child to stop before the controlled shell. Only the random, initially absent Compose project can be stopped. Its container PID namespace is stopped on bounded-collector failure; outer finally removes the owned project and independently checks container/network/volume labels, the former exact container ID, host children and settled pipes. No failed attempt is automatically retried. A name collision is retained and never cleaned as an owned project.

The migrated default `dispatcharr` database is only read by two eleven-table/eleven-sequence audits. They run before copying/owned seed work and in outer finally before teardown. Audit transactions are read-only; a connected database-name check precedes each phase. Only a new `rust_probe_notif_<10hex>` database is copied, seeded or changed. Both audit and exercise use PostgreSQL/Redis inside this one disposable container. The table scope is explicit, never a full database audit claim.

The fixture observes inherited UI settings before isolating the owned notification seed and never changes policy. It fails setup if actual loopback UI access is denied. `force_authenticate` bypasses JWT/API-key credentials; actual list/dismiss `IsAuthenticated` and network-aware create `IsAdmin` still dispatch through the real resolver. The seed uses real standard1/admin10 users and the original Hello/World SYSTEM notice. Every request repeats the owned database guard, resolver class/action check and exact slash path, accepts JSON, and follows no redirects.

The eight requests retain exact status/body bytes and hashes, JSON, headers, notification/dismissal rows, all named row/sequence fingerprints, own-clock bounds and bounded raw Django Redis cache inventory. Unrelated cache keys and TTL activity are retained. SYSTEM/no-condition scope does not exercise developer condition evaluation. SQL/cache evidence does not prove Channels/Celery delivery or silence. Predictions remain predictions until the real capture is compared. Unexpected outcomes and partial captures survive, and fail qualification without rewriting or rerunning them.

`controller/effects.py` is a pure comparator. Its synthetic controls never become Source observations; only the reviewed controller can label an actual capture. Positive create checks actual model defaults and its own Source clock interval. Repeat dismiss checks the same row/PK/timestamp and observed sequences. The copied input is never sequence-reset. Other role slices, inactive/streamer0, credentials, TCP/browser, other CRUD/actions, developer/expired/admin-only cases and Native/performance/full-role acceptance remain uncovered.

Local review commands (no Docker or Source execution):

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s controller -p test_controls.py
PYTHONDONTWRITEBYTECODE=1 python3 controller/run.py
bash -n controller/start.sh
```

Do not invoke `--execute-reviewed-seal` until Root has reviewed this draft and the exact seal. No repository files, canonical register, index or refs are modified by preparation.
