# M7 authenticated WebSocket and Stats on packaged d426d322

Packaged Rust `d426d322b27a197aaaaa057ef73dfb53c516c452` and pinned Original Dispatcharr `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` each passed three ordinary WebSocket observations. The paired Compose run and its independent reducer both exited 0. Original used the pinned Daphne consumer. Native used production Rust Main with optional jobs disabled.

- An authenticated administrator connected to `/ws/` and received HTTP 101 and the same connection-established envelope on both backends.
- Both replayed the same seeded cached IP snapshot when connecting. The event omitted the internal verification clock and preserved the cached entry.
- An ordinary authenticated GET to `/proxy/ts/status` returned HTTP 200 and an idle snapshot with zero channels. The nested JSON string in the real `channel_stats` update matched the corresponding HTTP JSON. Both sockets completed the normal close with code 1000.

Each owned user and cache entry retired. The cleanup check verified two Original and four Native role identities, their observed host generations, and empty project inventories. The receipt records typed observation hashes and no credential values. Active playback and resource measurements retain their separate scope.

[Machine-readable receipt](../scripts/rust-integration/m7-d426d322-websocket-positive.json)

Original Dispatcharr, Django Channels, Daphne and dependency authors retain full credit.
