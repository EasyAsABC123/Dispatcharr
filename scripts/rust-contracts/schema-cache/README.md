# Original schema-cache test assignment

Original Dispatcharr authors and dependency contributors retain full application,
algorithm and license credit. Exact pinned test/view/URL sources are included.

Three named successful tests are reused from the qualified unchanged Original
3,362-test suite: cache reuse, explicit invalidation, and eight-thread single-flight.
No new Source workload ran. Exact per-test successful log lines and assertion ASTs
are retained in observations.json and bound to the full-suite log SHA.

Only the existing api/schema/ METHOD_UNRESOLVED work item is assigned. Its method
declaration remains unresolved. The original tests use anonymous RequestFactory
direct view calls, mocked schema generators and LocMemCache. They do not measure
real full schema generation, TCP/credential backends, Redis/multi-process caching,
Swagger/Redoc or JSON aliases, complete roles, Native behavior or performance.
Whole-workflow assertions retain their original mocks and fixture context.

Offline validation (no Django or application imports):

```
python3 scripts/rust-contracts/schema_cache_contracts.py --repo .
python3 scripts/rust-contracts/test_schema_cache_contracts.py
```

An optional explicit --raw-log PATH checks the complete retained Original log hash
and all three exact success lines. There is no private-log fallback. The binding
fixture supplies the existing baseline runner command for a separately requested
fresh replay; no fresh replay is claimed here. Original full-suite provenance is
in ../framework-auth/full-suite-reference.json. Original AGPL terms are unchanged.
