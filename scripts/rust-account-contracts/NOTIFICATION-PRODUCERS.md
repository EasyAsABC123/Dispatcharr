# Original notification publication inventory

Credit belongs to the original Dispatcharr authors and contributors. This is a
source audit for the native M6/M7/M8 bridges, not a claim that the jobs below
are ported.

| Original producer | Database and publication behavior | Native state |
| --- | --- | --- |
| `apps/accounts/xc_auth.py` lockout | Failure10 logs login_failed; cooldown-gated warning replaces old inbox/dismissals; `system_notification` update. | Store original-source SQL/cache/envelope gate passed. Native M7 delivery separate. |
| `apps/accounts/throttling.py` login lockout | Denied login claims per-IP emit flag; one login_failed event plus longer inbox cooldown. | Process-local native login throttle exists; shared flag/event/inbox port remains pending. |
| `apps/accounts/authentication.py` API-key lockout | Failed-only budget crossing logs login_failed and attempts cooldown-gated inbox. | Process-local native budget exists; shared event/inbox port remains pending. |
| `core/tasks.py check_for_version_update` | Conditional create/update/remove version notices; publish new notice and notifications_cleared at original branches. | Scheduled native job pending. |
| `core/tasks.py create_setting_recommendation` | Update-or-create notice; publish only if created. | Scheduled native job pending. |
| `core/developer_notifications.py sync_developer_notifications` | Version/expiry JSON sync adds/updates/removes rows, then always sends notifications_cleared refresh. | Native sync/version/condition invalidation job pending. |
| `apps/m3u/tasks.py evaluate_profile_expiration_notification` | Expired/7-day warnings update-or-create and publish; remove opposing/stale keys with notification_dismissed. Bulk expiry task also cleans stale keys. | Native profile expiry job pending. |
| `core/api_views.py _perform_ip_lookup` | Cache and publish ip_lookup_complete on success or first negative lookup; failed known-data recheck only bumps freshness. |18 original-source HTTP/cache cases passed; native M7 delivery/browser workflow separate. |

`SystemNotificationViewSet` ordinary create/update/delete/dismiss actions have
no generic websocket publication hook in the original model, view or signals.
Do not add a broadcast there and call it source parity. Personal dismissals are
user-scoped and their existing REST behavior remains independent of the job
publications listed above. M3U/EPG blocked downloads log SystemEvent entries;
they are not an extra generic notification inbox producer.

The common notice envelope is `type=update`, `data.type=system_notification` and
`data.notification` containing id/key/type/priority/title/message/action_data,
is_active/admin_only and created_at ISO with UTC offset. Do not replace it with
the REST serializer shape (is_dismissed/source/expires_at differ). Native
producer hooks should pass the complete original envelope to UpdateEventSink;
M7 owns bounded ChannelsRedis compatibility and per-recipient admin filtering.
Configured Connect/plugin dispatch following system-event logging remains M8.
