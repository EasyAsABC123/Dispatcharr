# Actual production-core stop, mark and expiry trace

Original Dispatcharr authors and contributors retain application/algorithm credit and licenses. This functional gate is separate from comparative performance and whole-application acceptance.

The pinned Original StreamBuffer is compared with actual native production packet assembly, shared InputWriter, InputStore, Channel and History. The first13 records retain the original split/truncated input, complete-packet flush, per-PID DI stamping, reset, oldest/time cursor lookup, actual TTL, same-producer continuation and stop-rejects-writes/retains-Redis-reads cases. Ten additional actual Original cases cover reconstruction from an existing/persistent index, reopening after actual expiry, marking before any media, partial-only stop, complete-plus-partial stop, partial reset, and DI sidecar expiry while media remains valid. No prospective mathematical adapter is represented as executed Rust.

Comparator rules are in `trace_contract.py`: exactly23 named records in captured order, identical index/local-buffer lengths/DI sets/oldest and age0/age60 cursors, all chunk sequences/lengths/full synthetic hex bytes, and current reader DI status. JSON types are exact; bool cannot stand in for index1. The sole explicit cursor representation mapping is Original last-consumed+1 versus an observed Rust Viewer's next-to-read value, with actual `History::next` full-byte delivery. Source-only dataset fields naming hypothetical Native cursors remain reference annotations; the separate native dataset records real execution. SHA256 fields are redundant with the compared full bytes and are not used as a substitute for delivery. No rows are sorted or failures filtered after comparison.

## Production boundary changes

`Channel::finish` closes producer publication and preserves retained cache/reader admission until ordinary expiry; `Channel::stop` retains its existing manager-teardown behavior. The regular production InputWriter is used by Session publication. It serializes one owned publication, performs owner-fenced Redis storage and cache publication, and retains that slot/media/channel admission through an actual late reply. Producer finish waits for the physical slot; it does not roll back a late write. InputAccumulator's production switch/reset/finish helpers match Original complete and partial packet handling, including Original's partial-only100-byte stop remainder.

The persistent producer index is read under the current coordinator fence and seeds a fresh cache's canonical position without inventing media. Its local index is the actual Session counter shared with the writer. InputSnapshot continues to hide expired/unproven publication-era media; its index can remain0 while the producer's local/persistent index is4. Those are different observation layers, not a changed owner-safety policy or a fabricated cursor.

Source sidecar marks are published at the next index before media arrives. Source-mode publication does not recreate or refresh an expired sidecar. Existing legacy InputStore put behavior remains available. A marked cached frame performs a bounded payload-free sidecar lookup, so a DI key that expires before its media does not leave stale cached metadata. Unmarked cached frames keep their fast path. Metadata/history/snapshot reads retain frame, viewer, client and channel scopes through physical Redis replies. Session-to-writer ownership retains the channel permit directly, without a Session cycle.

## Executed physical coverage and limits

The trace forces a real Redis History miss, single-frame backpressure and final-viewer retention. Real Redis pauses exercise canceled publication/finish, logical timeout with a late write, and delayed cached-metadata/historical reads. Local index/cache do not fabricate success for an ambiguous late commit; finish waits for actual scope release. Existing shared-input HTTP11 and InputStore14 controls exercise ownership/follower shutdown, held HTTP frames, provider/client limits, retention diagnostics, GC, time selection using actual Original Lua (including equal scores), expiry and stale-owner cleanup fences.

StreamBuffer itself has no coordinator-owner concept. Native successor/stale-owner guards are explicitly safety controls; they are not claimed as an Original manager ownership protocol capture. Manager stop/cleanup stays separate and unchanged. In particular ordinary InputStore cleanup can returnfalse after its temporary publication-owner key expires; the delayed-marker fixture records that fact rather than falsely claiming immediate manager cleanup. Existing bounded GC tests cover eventual receipt-based reclamation separately.

Redis/Lua can mutate owned state after a logical timeout. These paths remain operational/ambiguous, not synthetic Source-success results, and there is no general cancellation rollback claim. Atomic native storage versus Original nontransactional pipelines, arbitrary backend/network failure ordering, and expiry/ownership transitions racing between separate payload and metadata reads are not asserted to have complete Source failure parity. The23 controlled normal traces and explicit retained-ownership controls are the accepted functional domain. No old benchmark gain transfers to this changed production core; a new source-bound optimized benchmark is prepared separately.

## Reproduction

Use committed `scripts/rust-core-perf/support`, whose entire31-file map is verified before helper imports. There is no private support fallback. The controller archives exact Original revision1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430, checks archive SHA256f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff, creates a guarded copied database and internal Compose network, and audits the default fixture database before/after. Test bodies execute inside the same pinned Original image. Native peers and their complete source map must be bound by an explicit build record; binaries are retained outside Cargo target output.

Install the pinned Rust toolchain and cached dependencies first. Freeze a qualified committed revision and build the peers without reading mutable Rust files:

```sh
python scripts/rust-core-contracts/build_native.py \
  --revision <qualified-core-commit> --output /tmp/core-build \
  --target /tmp/core-cargo-target
```

This resolves the revision to an immutable commit, extracts its Rust tree, copies committed external include fixtures, runs two separate locked/offline Cargo `--no-run --message-format=json` commands, identifies all six real executable artifacts, copies them to `/tmp/core-build/peers/`, and writes `/tmp/core-build/build-record.json` plus source/command/compiler/profile hashes. Generated Cargo output is outside the frozen source; source hashes must remain unchanged through compilation. The normal committed-revision command has no private artifact prerequisite. An optional explicit `--overlay-manifest` with required `--overlay-manifest-sha256` supports reviewed precommit core-only snapshots; it is recorded, not silently captured from mutable source. Unknown overlay paths, unsafe archive entries, changed overlay hashes and nonempty output are rejected.

Run against that exact build (the compile root comes from its build record):

```sh
python scripts/rust-core-contracts/run_native.py \
  --output /tmp/new-core-trace \
  --compile-root /tmp/core-build/source \
  --build-record /tmp/core-build/build-record.json \
  --binary /tmp/core-build/peers/native-core-peer \
  --extra-binary /tmp/core-build/peers/http-peer \
  --extra-binary /tmp/core-build/peers/input-gc-peer \
  --extra-binary /tmp/core-build/peers/input-redis-peer \
  --extra-binary /tmp/core-build/peers/input-retained-peer \
  --extra-binary /tmp/core-build/peers/input-source-expiry-peer
python -m unittest discover -s scripts/rust-core-contracts -p test_trace_contract.py
```

`--compile-root` is the parent of rust/ used to compile these peers. It permits the existing input age test's compiled source lookup to read the immutable actual Original buffer source; only empty intermediate directories are added. Additional Redis tests use disposable validation DB7, so their FLUSHDB controls never touch Source cache DB0. No external traffic, recording, production restore, resource measurement or app-image transfer occurs.

Raw native7 retains an inherited Source-only scope string in integration.json despite real native execution. The verified handoff/public summary is authoritative about the actual Source/native23 scope; raw successful tools remain unchanged. Portable publication uses a separately captured runner and verified committed support. Failed setup attempts and actual counterexamples remain linked in the summary; none are silently overwritten.

Validator hardening after portable9 requires explicit field-presence equality (including nullable fields and chunks) and successful Original cursor byte verification. The captured23 rows are complete and pass; executed9 tools remain immutable. Pure missing-null/chunks and false-Source-verification controls cover the hardened derivative.

## Sidecar-only pending marker boundary

The source-bound invariant is **at most max_chunks published payload/timestamp entries, plus one pending next-index DI sidecar member**. Source `mark_discontinuity()` records that next index before any next media exists. SNAPSHOT, PUT, MARK_NEXT and retained diagnostics must allow this sidecar-only+1; tightening it to the payload limit reintroduces a within-budget failover rejection. Payload count, timestamp count and byte caps are unchanged.

At max_chunks2/752bytes, Original has two retained376-byte payloads and accepts marker3, producing DI[1,2,3] while payload remains752bytes. Frozen7 incorrectly returned Capacity/DI[1,2]. The corrected eight-path snapshot matches that five-step actual Source trace. Its explicit controls reject more than one extra sidecar, preserve successor ownership, retain canceled marker/finish admission through the actual reply, and keep a subsequent third publication bounded to two payloads. Original's three-payload1128-byte result is captured separately as **outside** the common native budget; it is not filtered away and not called matching work.

```sh
python scripts/rust-core-contracts/run_native.py --scenario pending-marker \
  --output /tmp/new-core-pending-marker \
  --compile-root /tmp/core-build/source \
  --build-record /tmp/core-build/build-record.json \
  --binary /tmp/core-build/peers/native-core-peer
```

Use a qualified revision containing the sidecar-only fix and nested pending-marker QA peer. `source_buffer_boundary.py` is the pinned Original observer; `compare_pending` declares the exact ordered five-record common domain and requires the separate over-budget Source witness. No payload or timestamp limit is raised to make this comparison pass.
