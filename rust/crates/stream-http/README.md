# Native live MPEG-TS HTTP slice

This derivative port follows the original Dispatcharr implementation. Full credit
for the original application belongs to its authors and contributors; see
[ATTRIBUTION.md](../../../ATTRIBUTION.md). This crate is an implementation slice,
not a complete live-proxy compatibility claim.

`StreamEngine::router(Arc<dyn PlaybackResolver>)` serves native TS playback at
`/proxy/ts/stream/{channel_id}` and protected status/stop routes. The resolver runs
for every viewer before channel reuse, and separately authorizes every admin
request. The host application supplies database/authentication policy; no anonymous
fallback is implemented by this crate. Only HTTP(S) native proxy sources are
supported. Unsupported output formats return 501 after authorization.

The registry creates one upstream request per channel, shared by every viewer.
It bounds channel count, viewers per channel, retention bytes/age, publish chunk
size, initialization timeout, upstream read/connect timeouts, and retries. Input
is reassembled into 188-byte TS packets, batched into shared immutable payloads,
and flushed on a bounded timer. Reconnects reset partial input and apply the legacy
first-packet-per-PID discontinuity helper. A slow reader cannot stop ingestion.

Eligible alternate stream/provider candidates remain in original channel ordering.
Native HTTP failure exhausts the current source's retry budget before moving on;
capacity-full candidates are skipped without an upstream request. The provider
slot is cleared before reserving the next candidate, while one fenced channel
owner remains held throughout. Status reports the currently selected stream and
provider rather than the initial source. The old source's incomplete TS tail is
dropped before marking the first new-source chunk.

Defaults port Python's three total attempts, 1,800-second failure-idle reset,
30-second stable-playback rotation reset, 60-second exhausted-list cooldown, and
ten cooldown wraps. Rust's `max_retries` counts *additional* attempts, so its
default is two. Stable playback resets source-rotation bookkeeping, not the
connection-failure history. Before any media succeeds, exhausting the list fails
without wrapping. After successful media, exhausted rotations wait interruptibly
and stop at the configured limit. A configured maximum of 256 candidates bounds
the per-channel selection state; exceeding it fails rather than truncating silently.

The Hyper adapter uses `Bytes::from_owner` with the runtime's owned delivery
lease. Each viewer may have one outstanding payload, including clones held by
Hyper; requesting another payload waits for the previous lease. Every HTTP frame
also retains the channel admission permit, so stopping/recreating channels cannot
bypass the global bound while older body frames remain alive. Viewer and frame
drop both trigger final-viewer cleanup after the configured shutdown delay.
Explicit stop/shutdown cancels pending upstream requests and releases retention.

The bound covers media payload retention and frame ownership, not OS socket
buffers, TLS state, allocator metadata, HTTP headers, or runtime overhead.
Defaults permit 32 channels, 400 viewers/channel, and 16 MiB retained/channel;
deployment-specific limits must be chosen with the full process-memory bound in
mind. Memory admission is per process. Provider capacity and channel ownership are
coordinated through Redis; cross-replica media delivery remains open.

`new_with_coordinator(config, Some(coordinator))` acquires a fenced owner/provider
lease before starting the upstream. Local viewers share one reservation; another
replica cannot open the same channel or exceed the legacy provider counter. Lease
loss closes the upstream; stop and final-viewer drop release the exact reservation.
Without a coordinator, capacity-limited profiles fail closed with 503, while
unlimited native sessions remain process-local. Shared ServerGroup credential
profiles still require resolver metadata and remain disabled at the app boundary.
`new_with_services` additionally accepts the client store for atomic user limits,
legacy viewer metadata, batched rate telemetry, and immediate admin-stop cleanup.
Owned HTTP frames retain the client admission lease. Fenced channel metadata is
mirrored once per second. Status projects the basic and detailed legacy fields;
Rust media chunks live in bounded memory, so Redis payload-key diagnostics report
no Redis chunks. The app dispatches native HLS to the separate `hls-http` gateway.
Native TS reconnects emit the original 188-byte keepalive packet every 500 ms,
with a five-minute budget since the last media. Internal HLS input readers do not
register phantom TS viewers or emit keepalives.

Remaining gaps include public Xtream aliases, profile transforms, fMP4, complete
realtime events and media analysis, cross-replica media delivery, distributed
status enumeration, and full differential status coverage. Initialization still
waits for media instead of immediately sending the legacy initialization response.
These remain explicit M2/M3/M7 acceptance work.

Run the crate's tests with Rust 1.90.0:

```sh
cargo test --manifest-path rust/Cargo.toml -p dispatcharr-stream-http --locked
cargo clippy --manifest-path rust/Cargo.toml -p dispatcharr-stream-http --all-targets --locked -- -D warnings
```

Loopback tests require permission to bind local sockets. They cover authorization
before reuse, shared upstream ownership, HTTP body-frame admission leases,
slow/fast viewers, bounded retention, final-viewer shutdown, initialization
timeouts, upstream HTTP errors, retries, and shutdown capacity closure. Fixture
packets test transport/alignment only; actual decoder checks and paired Docker
performance measurements belong to the application-level verification stack.
