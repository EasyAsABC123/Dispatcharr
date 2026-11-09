# Streaming fanout runtime

This crate provides bounded asynchronous delivery over `stream-core`. It has no
HTTP server, database, Redis ownership protocol, encoder, or application auth.

Create a `Channel` inside a Tokio runtime with retained byte/age limits and a
maximum viewer count. Publish shared opaque chunks. Subscribe at live head or a
time offset; each viewer has an independent next-to-read cursor. `next().await`
returns a chunk, an explicit lag event, cancellation, or channel shutdown.

`Delivery::Chunk` borrows the viewer's single owned chunk slot. Finish sending it
before calling `next()` again. This keeps the runtime free of per-viewer media
queues and avoids cloning payload bytes. Transport code must preserve this bound
and must not copy deliveries into unbounded downstream queues.

The retained payload cap is per channel. In-flight viewer deliveries can outlive
retention; a conservative bound is `retained_limit * (max_viewers + 1)` payload
bytes when every viewer holds a different maximum-size chunk. Typical fanout of
the same chunk shares one allocation. Future transport configuration should also
cap maximum chunk size, global channel count, and socket buffers.
The bound excludes ring/viewer metadata, runtime tasks, allocator overhead, and
transport buffers. An HTTP adapter requiring owned body frames (for example,
Hyper) must introduce explicit delivery leases/accounting before transferring
ownership; simply cloning payloads into body frames would bypass the borrowed
single-slot contract.

A periodic owner task expires media during upstream stalls, even with no readers.
Reads/writes also expire retention synchronously. Notification waiters register
before inspecting state to avoid missed publish, cancellation, or stop signals.
Every buffer lock is short and synchronous; no await or network operation holds it.

Cancellation wakes a viewer but its slot remains reserved until the `Viewer` drops,
because a cancelled viewer may still hold an in-flight payload. A cancellation
handle does not retain a viewer slot or channel owner. `Channel::stop()` clears
retention immediately, closes subscriptions, and wakes pending readers; a currently
borrowed in-flight delivery remains valid until its viewer releases it. Channel
owners must stop explicitly as part of their lifecycle.
As a fallback, dropping all channel/viewer owners releases the weak worker and
buffer within one timer period (at most one second). Explicit stop releases
retention immediately.

Run `cargo test -p dispatcharr-stream-runtime`. Paused-time tests cover 100-viewer
payload sharing, slow/fast independence, subscribe/publish races, registration
before notification, abandoned reads, stop/cancel waits, idle expiry, and permit
release. These are runtime contracts, not proof of full playback compatibility or
whole-application performance gains.
A four-worker stress test also synchronizes 100 subscribed readers against a
concurrent publisher of 1,000 chunks, checking every sequence and byte with bounded
reader timeouts. It complements deterministic paused-time tests with real worker
concurrency and does not measure HTTP throughput.
