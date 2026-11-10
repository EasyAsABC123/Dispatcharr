# Bounded cold XMLTV batches

All application credit remains with the original Dispatcharr authors and contributors. Existing AGPL licensing is preserved. This post-image25 change is a source-bound functional fix for cold command amplification, not a performance acceptance result.

A ready-cache read batch alone cannot amortize a cold leader's work when only one64KiB stored member is published before each read. The actual before fixture confirmed one leader/nine followers:204 write calls and748 read calls for identical13,015,458-byte exports. Raw Redis saw204RPUSH,965EVALSHA,2788LINDEX,2343GET and7714TYPE calls. The original fixture used29RPUSH and282LINDEX calls. Read calls include building followers' polls; counts are observations from that functional run, not universal scheduling constants or CPU attribution.

The change keeps the preamble and200-channel phase yields unchanged. Real programmes collect up to512KiB, capped by the existing maximum frame limit. Delivery splits only the current available value into valid UTF-8 members of at most64KiB and publishes a bounded group (at most32 members) with one fenced Lua RPUSH, followed by the existing bounded read. It does not look ahead into the source generator or implement per-programme renderer templates.

The write script checks ownership, catalogue/per-export byte budgets, member sizes and list-count limits. If a later member is invalid or reaches a quota, the valid prefix is committed and reported explicitly. Delivery returns that prefix before reporting a deferred capacity error and performing fenced cleanup. A bad later member does not erase already-deliverable bytes or become a successful truncated export. Physical argument buffers and ownership guards survive caller cancellation until the actual Redis reply and abandoned-leader cleanup finish.

`verified-xmltv-cold-native-1.json` binds the two frozen source/tool snapshots and raw command-count artifacts. After the change, the same fresh-fixture workload produced the same complete original/native XML and recorded30write calls,414read calls (273data,131wait,10EOF),457EVALSHA,2444LINDEX,1104GET and3650TYPE calls. One leader/nine followers remained. Stored boundaries changed from204 to203 members; the original had29 larger members. The native member maximum remains64KiB and Python cache interoperability remains passing. These count changes alone do not establish a CPU or latency improvement.

The after gate passed47 original/native HTTP cases, actual Python-written-cache replay with no extra audit,9audit checkpoints,36 complete generator cases,18real Redis/SQL/HTTP tests,5native unit tests and strict all-target Clippy. New tests compare valid-prefix/error ordering under small single/batched policies; exercise malformed next members, UTF-8, chunk quota and stale fences; and pause actual Redis while cancelling the leader to prove bytes/ownership persist through its physical reply and eventual cleanup. The existing100-reader/slow-final-frame tests also remain passing. Owned Compose resources were removed and shutdown restored the full request memory budget.

Cold pending data stays within the configured frame bound and existing build reservation. Physical writes reserve four times their bounded payload amount before cloning/queuing arguments; reads keep their existing twice-reply-byte and single-frame leases. Source error timing outside the specifically tested atomic-prefix cases remains a separate fault-compatibility gate: changing programme batching can change how much unflushed text exists when malformed database metadata causes a later generator exception. No full malformed-database or whole-milestone parity claim is made.

Reproduce the functional command-count gate (after execution authorization):

```
source artifacts/rust/tooling/environment.sh
XMLTV_CARGO_TARGET_DIR=/home/justin/github/Dispatcharr/artifacts/rust/xmltv-cold-cargo-target python scripts/rust-xmltv-contracts/source.py --native-cache --native-content --native-http --cold-cache-diagnostics --output artifacts/rust/m7-xmltv-cold-diagnostic-N
```

The command uses a unique Cargo target and freezes runtime/tools before Compose setup. No app image is built. The parent builds the canonical image, runs packaged regressions, then schedules a short isolated cold10 diagnostic and any new paired matrix under unchanged budgets. Failed image25 performance evidence is retained and cannot be relabeled as passing because command counts improved. Remaining read-side Lua payload/catalogue work is not removed speculatively; further changes require new evidence.
