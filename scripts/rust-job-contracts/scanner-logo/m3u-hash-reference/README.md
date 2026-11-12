# Original M3U hash-setting lookup

Original Dispatcharr/Django/Celery/Redis/dependency authors retain code, algorithm and license credit. This packet has no Native/Main acceptance claim.

Source70/25 comprises22 actual model+PostgreSQL+Redis lookup probes, one actual held cached reply, and two actual scanner→ordinary groups/single worker cases. A separate13-check real worker case qualifies JSON null for Single. Factory/model probes are explicitly not task execution.

The group is `stream_settings`, field `m3u_hash_key`; shared Django cache dictionaries are authoritative, including `{}`. Missing/nonobject/empty SQL groups use stream defaults with empty hash. Account parsed `custom_properties.hash_key` wins if nonempty, otherwise falls back globally. None gives no fields; CSV strips, drops unknown tokens and deduplicates known `name,url,tvg_id,m3u_id,group` in order. Lists/tuples raise TypeError. Actual empty/null workers return normal results with the global empty hash; actual url worker imports a different stream hash. No general hash implementation is proposed here.

The cached-url/DBempty probe uses an owned client-route override to identical Redis db0 via a reply relay. It remains pending beyond2.2s and returns cached `['url']` only after release4.2s. Native's legacy logical-timeout DB fallback therefore cannot be treated as authoritative empty-hash admission. Source's exact code/package hashes, raw settings/cache bytes, SQL query shapes and actual task/result metadata are retained.

Reproduce the25-case reference:

```sh
python3 scripts/rust-job-contracts/scanner-logo/m3u-hash-reference/test_source_plan.py
python3 scripts/rust-job-contracts/scanner-logo/m3u-hash-reference/run_pair.py --repo . --support-root scripts/rust-core-perf/support --output artifacts/rust/m3u-hash-source-rerun
```

Run `null-worker/run_pair.py` with the same arguments/fresh output for the separate real null worker witness. These use pinned Original1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430, internal-only networking, copied disposable PostgreSQL/Redis8+9 and synthetic local files. No real providers/user files, Native resources or performance measurements. The intended guard is read-only/no cachefill, conservative on unknown types/cache uncertainty, and refuses unported nonempty configuration before ACK; it does not claim Source cachefill/outage/concurrent configuration parity.
