# Proposed correct-route Original notification fixture — NOT EXECUTED

Pinned Original1241b3f defines `/api/` in dispatcharr/urls.py → `core/` in apps/api/urls.py → router basename `systemnotification` in core/api_urls.py. Actual list/create is `/api/core/notifications/`; dismiss is `/api/core/notifications/<pk>/dismiss/`. The existing three tests instead construct `/api/system/notifications/...` with APIRequestFactory and invoke as_view directly. Keep those original tests unchanged and excluded from route assignments; do not pretend the wrong URL was resolved or normalize it.

Recommend one small Source-only in-process APIClient fixture through the real Original resolver/DRF dispatch, not direct as_view. Use only a new owned copied PostgreSQL database and its owned Redis. Prefer the Original `manage.py shell` path for controlled initialization: shell is actually in should_skip_initialization's command list; test is not. Core signals import happens before the skip. This is explicitly management-mode framework routing/permissions/SQL evidence, not real TCP, production uWSGI startup or authentication-backend acceptance.

First record reverse/resolve identity for the exact routes and `SystemNotificationViewSet` action mappings. Record wrong-path resolve/Resolver404 without assuming404, because the frontend catch-all can resolve it. Never send or assign the wrong route. Three candidate stable operations only: list GET route-025c0d74fa99218d4728:GET, create POST same route-ID, dismiss POST route-1577c7cc68b36d7ee6f4:POST. No registration now.

Seed the same Hello/World active, non-admin-only Source.SYSTEM notice and real admin10/standard1 users as the Original tests, but only in the owned copied DB. Use force_authenticate and label the JWT/API-key backend bypass. Add one anonymous client. Accept JSON, use exact slash routes and disable redirects. Real Original read/dismiss permission is DRF IsAuthenticated; create permission is network-aware IsAdmin. Do not replace either. Use/record owned-loopback UI access settings. If setting preparation triggers real CoreSettings cache invalidation/developer-notification sync, record it as setup BEFORE clearing/seeding notification tables within the owned copy; do not patch those signals or hide them as request effects.

Eight fixed requests are proposed, with Source-code-derived predictions still unmeasured:

1. Anonymous GET list: expected401 (requires actual capture).
2. Standard GET list:200, seed visible/is_dismissedfalse.
3. Standard POST create with the original denial payload:403, no notification/dismissal mutation.
4. Standard POST dismiss:200, exactly one per-user dismissal, action_takenNone, notification row unchanged.
5. Repeat identical dismiss:200, same dismissal PK/timestamp/no duplicate; sequence behavior must be captured, not assumed.
6. Standard GET list after dismissal:200, seed absent/count0 with isolated one-notice seed.
7. Admin GET list:200, same seed still visible/is_dismissedfalse for that user.
8. Admin POST create: predicted201 from ModelViewSet/serializer; capture exact defaults/SQL and own-clock created_at interval. This positive is implementation-derived, not a reused historical assertion.

Capture per-request exact body/status, affected notification/dismissal rows+sequences and Source seed IDs, preserving unexpected outcomes. The response serializer and database defaults are pinned. Existing condition-cache behavior is deliberately out of this SYSTEM/no-condition seed scope; capture relevant owned condition keys if a bounded helper is available, and retain unrelated timezone/group cache activity rather than asserting global silence. Static Source search found no explicit SystemNotification/NotificationDismissal signal receiver, but SQL alone does not prove no Channels/Celery event. No broker-delivery assertion is proposed without a separate collector.

Default DB audit must include the existing nine named tables plus core_systemnotification/core_notificationdismissal and all eleven corresponding sequences, before any owned seed/API work and in outer finally before teardown. Compare exact row/sequence fingerprints; label the named audit honestly. Assert current_database/owned DB name before every seed/request span. Never seed, clear, delete or reset default dispatcharr data. No shared or user application instance is involved.

Use existing bounded subprocess collection and owned supervisor stop/connection-close helpers. Keep all external I/O on the owned internal network/local PostgreSQL+Redis; no provider URL, external HTTP, media decoder, native process or resource sampler is needed. Finally reap the shell/pipes, stop owned fixture services, remove only owned volumes and independently inspect exact project containers/networks/volumes/process absence. Any failure or uncertain cleanup is retained, never retried automatically.

Missing coverage remains explicit: streamer0/inactive and other role slices; retrieve/update/delete; dismiss-all/count/include_dismissed/action_taken/expiry/admin-only/developer-condition branches; JWT/API-key credentials, TCP/browser/Native/performance and full endpoint/M0/M1 acceptance. This proposal stays separate from the approved existing19-operation historical cohort. No tests, Docker, Source requests, registration or builds were performed during quiet.
