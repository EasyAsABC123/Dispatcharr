# Realtime transport migration

Original Dispatcharr authors and contributors retain full application credit;
see [ATTRIBUTION.md](../../../ATTRIBUTION.md). AGPL-3.0-only remains.

This crate is under construction. Source behavior is defined by
`dispatcharr/consumers.py`, `dispatcharr/jwt_ws_auth.py`, `core/utils.py` and the
actual pinned `channels_redis` runtime. An unchanged React build and working
REST routes do not establish browser event parity.

The adapter retains Source group members, per-channel recipient lists,
expiry, 12 random prefix bytes and MessagePack envelopes. One bounded receiver
loop per process replaces per-viewer polling. Authentication captures an active
access-JWT user once at the handshake, without REST API-key or network-policy
fallback. Admin-only telemetry and notification filtering applies per event.

Remaining gates include native WebSocket route equivalence, two-gateway
fanout, bounded slow-reader sockets, browser reconstruction, preview semantics,
producer wiring and frontend workflows. No live event performance or full
application parity claim is made.

The actual pinned Source Daphne app now passes 55 wire assertions covering
query-JWT precedence, roles, inactive/missing users, Origin handling, initial
and cached-IP envelopes, per-event visibility, falsy data, role/token changes
after handshake, ping/pong and malformed-message close 1011. Authentication
failures reject the upgrade with 403. All authenticated levels connect, including
level 0, and cross-Origin requests are permitted. Live principals are not
reauthenticated. The fixture found four stale group members after four consumer
exceptions, with 86,397 seconds left on the group key; healthy closes left none
before those exceptions. Rust will clean its own exceptional sockets promptly
as an intentional internal resource improvement. Source wire and cleanup
quality are recorded separately in
`../../../scripts/rust-realtime-contracts/verified-source-wire-1.json`.

The explicit bounded JSON-compatible MessagePack codec passes 307 byte-exact
vectors from the actual Python 3.13.18/msgpack 1.2.4 runtime, plus hostile-length,
depth, trailing-data and numeric-overflow guards. Binary/extensions/nonfinite
floats are outside this JSON event contract. Rust WebSocket route proof remains pending.

Six interoperability checks now pass between the original Python application
and two independent Rust process peers: both original publishing helpers, both
Rust publishers, direct-channel delivery, and disconnect isolation. The fixture
records unrelated original startup notifications rather than assuming an empty
bus. Four real-Redis cancellation/ordering tests also pass, including delayed
JOIN/POP replies, backup replay, capacity drops and admission retention. See
`../../../scripts/rust-realtime-contracts/verified-channels-interop-1.json` and
`../../../scripts/rust-realtime-contracts/verified-msgpack-codec-1.json`.

The native gateway now passes 61 component assertions from a frozen complete
Rust tree against the actual original Django authentication/cache/event
producers. This includes two gateways with 100 real sockets, role filtering,
query token precedence, cached IP replay, token expiry, malformed-input close
codes, and exceptional membership cleanup. Both gateways return all admission
and byte leases after shutdown. A deliberately unread socket is removed after
2.402 seconds while a fast reader receives the same 6MiB event in 0.157 seconds;
these are functional deadline observations, not resource-performance claims.

Actual Chromium receives the 6,291,516-character JSON event as one `onmessage`,
with the full 1MiB value intact, then closes cleanly with code 1000. Outgoing
16KiB protocol fragments share one serialized event ring with globally leased
wire versions and fixed 64KiB per-socket output admission. The source defaults
are retained for 1MiB incoming messages/frames, 20-second pings, 30-second pong
timeouts, 5-second upgrade waits and 24-hour connection lifetime. See
`../../../scripts/rust-realtime-contracts/verified-native-gateway-fanout-1.json`.

Main-app wiring now attaches the event sink before authentication service
clones and drains the gateway in parallel with media. The frozen IMAGE15-2 Docker app now passes the same 61 wire/Chromium/fanout
assertions. Both replicas close held sockets with code 1001 and exit without
OOM or drain warnings; shared membership returns to zero. All component code
hashes match the separate internal byte-budget proof (README changes only).
An 18MiB JSON event establishes backpressure through container forwarding and
removes an unread socket within 2.398 seconds while a fast reader completes in
0.164 seconds. The earlier 6MiB pressure probe did not establish that deadline
and remains preserved; no runtime timeout was relaxed. See
`../../../scripts/rust-realtime-contracts/verified-packaged-gateway-image15-1.json`.
The complete preserved React workflow remains open. Admin regex preview is deliberately
unimplemented pending its separate third-party Python `regex` compatibility
proof. Live/media lifecycle producers, all event families, ingress boundary
faults and native handshake edge cases remain separate gates. No live-event
resource gains or complete M7 parity are claimed.
