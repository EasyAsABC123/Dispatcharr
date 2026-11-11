# VOD provider and episode actions

The production-source component exposes `VodReads::provider_router()` for Movie providers, Series providers, and Series episodes. It shares the existing reader, two active workers,192MiB budget, queued input, physical authentication ownership, body leases, and shutdown. Main attachment, canonical packaging, and actual React modal validation remain separate parent gates. No extra memory pool or shutdown service is added.

The Source/native gate compares150 actual TCP observations, including Basic-user access, flags/adult visibility, object filtering, missing/bad identifiers, methods, HEAD, JSON indentation, and selected intrinsic headers. These actions use Authenticated permission; ordinary lists remain Standard. OPTIONS and unsupported methods retain Source admin checking. Full nested account/category/quality fields are present; nested serializers have no request context, so logo URLs are relative and account passwords stay absent even for admins. These are ReadOnlyModelViewSets: no new create/update/delete semantics are implemented.

Series episodes include inactive-only/providerless entries, while their nested provider relations filter active accounts. All episode relations are prefetched before the first item is serialized. Per-episode Series/Logo field caches survive nested provider serialization, while method counts/names and the account's four profile-query phases remain fresh. The first nested account seeds Source's shared stream-count context; later accounts may report zero despite their own streams. A separate Original Factory+real-SQL mutation witness verifies those phase rules against actual native TCP. That witness is not Source network authority. Two additional actual Original/native TCP cases validate nonempty cron fields with disabled fixture tasks and no worker execution.

Provider relation lists have no Source ordering clause. A fixed policy selected ALL25 multi-row Movie GET cases before the successful replay, rather than selecting the24 historical mismatches. Each uses fresh Original HTTP on the same post-replay database state, an exact UUID/database sentinel, and unchanged full18-table fingerprints before/after Source and native. Historical observations remain retained. Only historical per-relation order and the observed first-account stream-count derivation may differ; final Source/native status, headers, and raw bytes compare exactly. No sorting, invented identifier tie-breaker, whitespace, or whole-body normalization is used.

Native5 passed the four groups together:150 TCP; one controlled Factory/SQL body vsnative TCP; two cron TCP; and actual unstaggered mixed10 Original/native requests plus physical controls. A400ms Redis pause and a PostgreSQL relation lock retain68MiB of admission credits and the HTTP slot after caller abort, through real reply/rollback. Canceled/expired waiters have no read worker. Final body clones retain shared slots/bytes through shutdown. A1024-byte output limit returns503, never a partial or clamped200. Credits are not an RSS measurement. No performance gains are claimed.

The default128KiB row transfer guard remains; each raw list retains at most work_bytes/16 including vector capacity/per-row overhead, decoded nodes and account subarrays have additional explicit guards. Source unpaginated lists are not silently truncated: capacity failures return503. PostgreSQL temporary encoding remains in PostgreSQL's service resource scope.

Retained negatives: Source1 omitted the new frozen helper (existing465 Factory observations completed); Native1 had24 unordered provider body differences; Native4 exposed the incorrect category/account JOIN after a controlled account heap update. The fixed category projection uses the Source's7-column through-table query and fresh account FK dereferences. Earlier fingerprints in Source2/3/4 used DjangoJSONEncoder millisecond precision; final native5 uses exact microsecond guards. Historical artifacts are unchanged.

Remaining capabilities: provider-info is a separate cached-metadata/remote-refresh lane; no stale cache or forced refresh is faked. Action browsableHTML, uncaptured suffix/encoded-key routing, packaged global headers, and React modal workflows are not accepted by this component proof.

The exact successful fixture is under artifacts/rust/vod-provider-actions-native-5. The standalone runner is an exact-byte copy of its executed run.py under a distinct name, preserving Root's canonical runner. Use an explicit unique target per frozen source/output:

```sh
VOD_CARGO_TARGET_DIR="$PWD/artifacts/rust/provider-actions-replay-1/target" \
CARGO_BUILD_JOBS=2 \
python3 scripts/rust-vod-api-contracts/run_provider_actions.py \
  --mode http --native-provider-actions \
  --frozen-rust-source "$PWD/rust" \
  --output "$PWD/artifacts/rust/provider-actions-replay-1"
```

Original Dispatcharr authors/contributors retain application and algorithm credit.
