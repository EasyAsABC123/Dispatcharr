# Bounded XMLTV ready-cache read batches

All original application credit belongs to Dispatcharr authors and contributors. This post-image24 change preserves original cache keys, UTF-8 stored members, TTLs, generation suffixes, fences, catalogue/export limits and complete HTTP entities. It changes internal read framing only. The failed immutable image23 hot-cache performance pair remains failed; no new performance claim is made here.

A physical read now joins up to512KiB from at most32 adjacent member/slice visits. Existing64KiB stored members are unchanged. A legacy member larger than512KiB is sliced using the existing member/byte cursor; an allowed8MiB original member is not rejected merely for exceeding the batch bound. A bad later member leaves already-valid prefix bytes available first, then raises its explicit capacity error on the next read. Empty members cannot make the Lua loop unbounded.

Each read acquires twice the effective maximum reply bytes before sending Redis work, retaining those bytes through the actual physical reply and the final returned frame clone. The existing one-frame cursor lease and bounded transport/task guards remain in place. Effective batch size is capped by one quarter of the configured global read-I/O budget; hard batch cap is1MiB and default global read-I/O remains64MiB. HTTP actor cancellation/shutdown behavior and independent detached-frame memory ownership are unchanged. No larger cached catalogue or whole-export per-viewer allocation was added.

`verified-xmltv-batch-native-5.json` binds the actual frozen host component/tool sources and evidence. Original and native500-channel/50-guide exports contain22,500 programme elements and identical13,015,458-byte bodies/SHA. Original storage has29 members, largest574,800bytes; native storage has204 members, largest65,536bytes. On each unchanged list, the64KiB diagnostic policy needs200 READ-script invocations including EOF; the512KiB policy needs26. These are functional script-invocation counts, not measured performance improvements or a direct raw command trace of old image23. The old image23 reader used one stored member/slice per READ; its paths are separately documented in HOT-CACHE-CODE-AUDIT.md.

The gate also passed all47 original HTTP/status/body/selected-header cases, actual Python-written-cache replay with no extra audit,9 audit checkpoints,36 complete generator cases,15 real Redis/SQL/HTTP tests,5 native unit tests and strict all-target Clippy. One hundred bounded ready readers progressed while a detached slow512KiB frame held its lease; final clone release restored all bytes. Actual Redis delayed replies verify cancellation cannot release physical admission early. Generation/fence/expiry/corruption and shutdown tests remain passing. Both host gateways drained with zero active requests and the full512MiB request memory budget restored; the owned Compose project was removed.

Retained failures document two test-lifetime/boundary assumptions and one existing TYPE/GET observer TTL race; they were not relabeled as successful runs. Native1's old frame assertion expected one member per frame. Native3 held a completed leader acquisition while asserting drain, then skipped cleanup and caused a second capacity rejection. Native4's cache observer saw a key expire between TYPE and GET; the later observer skips such vanished keys. Final native5 is a fresh frozen successful run.

Reproduce after functional execution is authorized:

```
source artifacts/rust/tooling/environment.sh
python scripts/rust-xmltv-contracts/source.py --native-cache --native-content --native-http --shared-guide-cache --output artifacts/rust/m7-xmltv-batch-native-N
```

The core app still needs a parent-owned immutable image and packaged regressions before any paired resource rerun. Keep the same workload, budget and baseline. Custom dummy regex, unrelated guide REST gaps, plugins and whole-application migration remain separate milestones.
