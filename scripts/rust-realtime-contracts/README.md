# Original-source realtime contracts

Full application authorship remains with the original Dispatcharr authors and
contributors. See [ATTRIBUTION.md](../../ATTRIBUTION.md); the Rust port retains
AGPL-3.0-only. Protocol compatibility also relies on the original Channels,
channels_redis, MessagePack, Daphne and tungstenite projects.

These probes distinguish original-source behavior, native component behavior,
and packaged application behavior. Passing one does not complete M7 or establish
whole-application performance gains.

| Gate | Evidence | Result |
| --- | --- | --- |
| Original Daphne query JWT, Origin, initial/cache JSON, role filtering and close behavior | `verified-source-wire-1.json` | 55 assertions pass |
| Actual Python 3.13.18/msgpack 1.2.4 codec | `verified-msgpack-codec-1.json` | 307 byte-exact vectors pass |
| Original synchronous/asynchronous publishers and two independent Rust process peers | `verified-channels-interop-1.json` | 6 interoperability checks pass |
| Frozen native gateway, two gateways/100 sockets/Chromium/slow reader | `verified-native-gateway-fanout-1.json` | 61 assertions pass |
| Frozen IMAGE15-2 full app route, two replicas/100 sockets/Chromium/held-socket SIGTERM | `verified-packaged-gateway-image15-1.json` | 61 assertions and clean shutdown pass |
| Actual original TS add/remove/status-GET stats producers | `verified-source-live-stats-1.json` | Original envelopes and real media captured |
| Original basic-statistics projection and native global catalog | `verified-statistics-projection-source-1.json` | 8 exact channel fixtures pass |
| Original raw-input byte report clock/counter method | `verified-source-byte-reporting-1.json` | 103 sequences / 3,215 reads pass |
| Frozen IMAGE16 live producer/global GET/foreign replica/media/shutdown | `verified-live-statistics-image16-1.json` | 62 assertions, clean75-frame audio/video prefix pass |

The package proof binds image
`sha256:d5929a085ea262bfe24d3fe7eaee49f6cb69b6183a40cc7cc99da9cf82c7ece6`
to source tree
`0053aef6fe71043b6db52851cb0e2cf691788cf175fe10ae956b02b5934ffde7`.
Its gateway code matches the separately measured component byte-admission proof.
The app deliberately exposes no invented byte metric: packaged membership,
held-socket close codes, exit status and drain results are recorded separately.

Chromium reconstructs a 6,291,516-character JSON payload as exactly one
`onmessage`; the 1MiB value is intact and close code 1000 is clean. The packaged
forwarding path needed an 18MiB JSON event, still inside the unchanged 4MiB
MessagePack and 25MiB wire caps, to establish slow-reader backpressure. The fast
reader completed in 0.164 seconds and the unread member disappeared in 2.398
seconds. A prior 6MiB pressure probe failed its deadline and remains preserved;
proxy buffering is a hypothesis, not a proved exact buffer size or app defect.
Both packaged replicas closed held sockets with 1001 and exited normally in
0.302 seconds. These are functional observations, not comparative benchmarks.

Source queue details matter: actual `channel_layer.group_send` enforces its
capacity; the original `_gevent_ws_send` helper directly ZADDs and does not.
Native publishing applies its configured bounded capacity to both paths. That
is an intentional resource guard, not identical synchronous-helper overload
behavior. Likewise, original exceptional consumers left four members with
86,397 seconds of group TTL; native own-member cleanup returns to zero. Healthy
wire behavior and intentional internal cleanup improvements are separate facts.

The exact original status API is `/proxy/ts/status`, without a trailing slash.
The trailing-slash URL falls through to React HTML. Actual Source registration
already has `connecting` metadata and one client; after first media it is
`active` at index four. Removal emits a zero-client snapshot while the channel
still exists during shutdown delay. Source status GET publishes its exact
returned body as the nested JSON `stats` string. Background snapshot ordering
is not linearizable. `healthy` appears only for a process with a local owning
stream manager. The disabled old two-second Celery task must not be replaced by
an invented periodic native broadcast.

Reproduction uses an isolated disposable Compose project, actual original
schema/Redis, pinned baseline image and unique output directories:

```sh
python3 scripts/rust-realtime-contracts/source.py --output /tmp/ws-source-proof
python3 scripts/rust-realtime-contracts/codec.py --output /tmp/ws-codec-proof
python3 scripts/rust-realtime-contracts/interop.py --output /tmp/ws-interop-proof
python3 scripts/rust-realtime-contracts/gateway.py --browser --output /tmp/ws-native-proof
python3 scripts/rust-realtime-contracts/gateway.py --source-live-stats --output /tmp/ws-source-stats-proof
python3 scripts/rust-realtime-contracts/gateway_image.py \
  --candidate-image dispatcharr-rust-candidate:m5-auth-m7-websocket-image15-2 \
  --candidate-build-manifest artifacts/rust/candidate-build-m5-auth-m7-websocket-image15-2/build.json \
  --output /tmp/ws-packaged-proof
```

The local toolchain/Docker environment must be configured first. Browser probes
use an isolated temporary Chromium profile and actual DevTools protocol.
Capabilities enter over stdin; JWTs are never command arguments or saved test
results. Tool/source hashes and binary/image identities accompany every proof.

Remaining gates include VOD/Timeshift lifecycle producers, admin regex
preview (third-party Python `regex`, distinct from builtin `re`), complete React
workflows, development-port routing, ingress/handshake edge faults, all event
families and resource comparisons with equivalent side effects. The shared live collector and deferred callbacks pass the original component
oracles and frozen IMAGE16 native lifecycle gate. Post16 abandoned-owner
notification changes remain separately scoped until their packaged gate. The earlier scoped TS/API resource measurements do not
establish realtime or whole-application gains.


Incoming HTTP compatibility is recorded separately in
[INCOMING-HEADERS.md](INCOMING-HEADERS.md). The actual original/native raw-header
families pass 58 packaged image19 cases and 97 packaged image20 scoped-IP cases.
See [image19 summary](verified-incoming-headers-image19-1.json),
[scoped host summary](verified-scoped-ip-host-1.json), and
[image20 summary](verified-scoped-ip-image20-1.json). The document links retained
before/fixed attempts, the intentional malformed-auth rejection scope, and the
recorded fixture-only login-history isolation. These are HTTP/metadata gates,
not additional WebSocket, full-app or resource-performance acceptance.
