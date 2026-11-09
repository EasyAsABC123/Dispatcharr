# Shared output storage

This derivative ports Dispatcharr's original `OutputStreamBuffer` Redis key
layout and publish-after-write index convention. Original authorship and full
credit belong to the upstream authors and contributors; see
[ATTRIBUTION.md](../../../ATTRIBUTION.md). AGPL-3.0-only is retained.

`OutputStore::connect` creates one multiplexed Redis connection. `claim` returns
an `OutputLease` only after atomic fenced ownership acquisition. The lease writes
binary chunks, init media, state and playlist descriptors; renew and release
compare the same fencing token. Shared snapshots expose the index, descriptor
and bounded timestamp ledger for worker restart and cross-replica media reads.
Formats `hls`, `fmp4` and their `:p<id>` profile variants use the exact legacy
`live:channel:<id>:output:<format>` keys. A stale owner cannot append, publish,
renew or delete a successor's output. Redis Cluster is not supported.

Binary reads reserve the maximum possible segment bytes **before** sending the
Redis command. The returned `SegmentBytes` retains the actual byte permits;
`Bytes::from_owner` can transfer them into HTTP frames without copying. The
server-side length check rejects oversized values before returning media.
Channel output byte/count limits and metadata limits are checked before mutation.
New safety ceilings deliberately differ from the unbounded original edge cases.
Mixed Python writes retain their original semantics and do not adopt Rust
writer admission; interoperability does not impose a cluster-wide Rust memory
limit on other implementations.

Caller cancellation and deadlines do not release admission for a queued Redis
response. A bounded supervised task retains the operation permit, owned request
arguments and read byte permit until Redis responds or the socket disconnects.
A stalled socket intentionally keeps those permits and rejects further work
at capacity. This avoids an unbounded queue of abandoned media responses.
No automatic reconnect or stalled-socket recovery is claimed yet.

Defaults allow 16 concurrent operations and 1,024 bounded queued operations, 32 MiB per chunk/init read, 256 MiB of
held read data, 256 chunks / 256 MiB per output channel, 60-second ownership TTL,
and 100-second chunk TTL. Open segmenters and HTTP-local caches have independent
limits. State and init expire after one hour; owner-fenced release removes all
tracked media keys and descriptors.

Run the real Redis gates with:

```sh
python scripts/rust-hls-parity/http.py --suite output \
  --output artifacts/rust/NEW-OUTPUT-STORE-RUN
```

Five gates passed at `artifacts/rust/m3-output-store-2/evidence.json`: two-store
binary reads and takeover fencing; retained read permits, channel bytes and TTL;
corrupt index and oversized data rejection; fMP4 profile init/state isolation;
and actual Redis `CLIENT PAUSE` proving both aborted and timed-out reads retain
admission until the queued response finishes. This is component evidence, not
HLS/fMP4 gateway integration or performance verification.
