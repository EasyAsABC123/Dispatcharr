# Realtime transport migration

Original Dispatcharr authors and contributors retain full application credit;
see [ATTRIBUTION.md](../../../ATTRIBUTION.md). AGPL-3.0-only remains.

This crate is under construction. Source behavior is defined by
`dispatcharr/consumers.py`, `dispatcharr/jwt_ws_auth.py`, `core/utils.py` and the
actual pinned `channels_redis` runtime. An unchanged React build and working
REST routes do not establish browser event parity.

The component adapter retains Source group members, per-channel recipient lists,
expiry, 12 random prefix bytes and MessagePack envelopes. One bounded receiver
loop per process replaces per-viewer polling. Authentication captures an active
access-JWT user once at the handshake, without REST API-key or network-policy
fallback. Admin-only telemetry and notification filtering applies per event.

The original Daphne wire capture and actual Python/two-Rust-process Redis
interoperability gates pass. Native WebSocket handshake/event delivery, bounded
slow-client queues, browser workflows and resource measurements remain open.
No live event performance or full application parity claim is made.

The actual pinned Source Daphne app now passes55 wire assertions covering
query-JWT precedence, roles, inactive/missing users, Origin handling, initial
and cached-IP envelopes, per-event visibility, falsy data, role/token changes
after handshake, ping/pong and malformed-message close1011. Authentication
failures reject the upgrade with403. All authenticated levels connect, including
level0, and cross-Origin requests are permitted. Live principals are not
reauthenticated. The fixture found four stale group members after four consumer
exceptions, with86,397seconds left on the group key; healthy closes left none
before those exceptions. Rust will clean its own exceptional sockets promptly
as an intentional internal resource improvement. Source wire and cleanup
quality are recorded separately in
`../../../scripts/rust-realtime-contracts/verified-source-wire-1.json`.

The explicit bounded JSON-compatible MessagePack codec passes307 byte-exact
vectors from the actual Python3.13.18/msgpack1.2.4 runtime, plus hostile-length,
depth, trailing-data and numeric-overflow guards. Binary/extensions/nonfinite
floats are outside this JSON event contract. Actual original sync/async sends,
both Rust senders and targeted direct-channel delivery pass the three-process
Redis interoperability fixture, with zero group members after close. See
`../../../scripts/rust-realtime-contracts/verified-channels-interop-1.json`.
The native WebSocket route remains unwired and unverified in this checkpoint.
