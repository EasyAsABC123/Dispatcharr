# Realtime migration source audit

Original Dispatcharr authors and contributors retain full credit. This is a
source audit and test plan, not evidence of a functioning Rust WebSocket port.
`store::realtime` now contains an unwired authentication/visibility foundation.
Its two pure policy tests passed within the 38-test store unit suite, and strict
Clippy/workspace compilation passed. No real-transport result is claimed.
Authoritative files are `dispatcharr/routing.py`, `dispatcharr/jwt_ws_auth.py`,
`dispatcharr/consumers.py`, `core/utils.py` and `frontend/src/WebSocket.jsx`.

`/ws/` authenticates the first `token` query value using SimpleJWT access-token
validation and an active existing user. Missing/invalid tokens reject the
handshake. The middleware does not use REST API keys or the REST UI network
permission path; reusing generic REST authentication would change behavior.
The consumer captures its authenticated principal at connection time. Changes
to roles or activity during a live connection need explicit source comparison
before introducing periodic reauthorization.

After acceptance the consumer joins the `updates` group and sends exactly
`{"type":"connection_established","data":{"success":true,"message":"WebSocket connection established successfully"}}`.
A cached public-IP lookup, when present, follows as a `data` envelope with type
`ip_lookup_complete`; cache-only `verified_at` is omitted. Ordinary broadcasts
retain the complete event envelope, usually `{"type":"update","data":...}`.
The existing React client switches on `data.type` and uses these events to
refresh lists, progress displays, recordings and notifications.

Connection telemetry (`channel_stats`, `vod_stats`, `timeshift_stats`,
`vod_started`, `vod_stopped`) is admin-only (level >=10). System notifications
whose `notification.admin_only` is truthy are also admin-only. Other update
types are delivered to any connected authenticated user. These checks must
occur per message; telemetry can contain anonymous-playback identifiers,
upstream URLs and client IPs. Tests must keep those values out of reports.

The only inbound command currently handled is admin-only `m3u_profile_test`.
It returns `data.type`, `search_preview` and `result`, with source URL
transformation/regex semantics. URL inputs are capped at 8192 characters and
patterns/replacements at 255; the preview regex has a 100ms timeout. Invalid
types/oversized inputs echo a bounded safe URL. A Rust regex engine cannot be
substituted without testing Python-compatible pattern and replacement forms.

Cross-process delivery currently uses the channels_redis protocol, not plain
Redis Pub/Sub: `asgi:group:updates` sorted-set members, group expiry 86400s,
nonlocal channel prefix through `!`, a channel sorted-set key prefixed `asgi`,
12 random bytes followed by MessagePack, `__asgi_channel__` recipient lists,
and channel expiry 60s. `core/utils.py` also implements this directly for
gevent/Celery workers. A rolling Rust/Python deployment needs a verified wire
adapter or an explicitly coordinated transport transition. A local broadcast
channel alone would silently lose worker/replica updates.

Required gates are real handshake comparisons (access versus refresh token,
inactive/missing user, token precedence), exact envelopes, cached-IP replay,
admin/member filtering including falsy/unknown event types, M3U preview vectors,
Python-worker-to-Rust and Rust-worker-to-Python delivery, two-replica fanout,
bounded slow-client queues and disconnect/cancellation cleanup. Next publish
integration points include M3U/EPG refresh/progress, recording state, provider
account refresh, bulk channel creation, logos and system notifications. Until
those gates run, unchanged React assets and working REST routes do not establish
browser workflow parity.
