# Stored EPG source order

Application behavior and credit belong to the original Dispatcharr authors and contributors.

The original EPG source queryset has no explicit ordering. Its returned sequence is still visible to clients, so this migration does not substitute primary-key sorting. The three-row fixture updates its first source to attach a cron task, leaving two sources without tasks.

The same-database PostgreSQL plan diagnosis is reproducible with:

```sh
python3 scripts/rust-epg-contracts/run_query_order.py --output artifacts/rust/epg-order-diagnosis
python3 scripts/rust-epg-contracts/run_query_order.py --native-order --output artifacts/rust/epg-order-native
```

Use the disposable Docker/Rust environment documented by the adjacent EPG runners. Artifacts retain original source response bodies, SQL, `EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON)` output, frozen probe tools, and native source hashes. The private fixture response file includes synthetic authorization headers and must remain in ignored evidence directories.

Before the correction, literal-NULL and custom prepared queries returned `[3, 2, 1]`, matching Python. A generic prepared plan for `($1 IS NULL OR id=$1)` estimated a selective row predicate and chose left nested-loop joins, returning `[2, 3, 1]`. Python's corresponding query used right hash joins. Projection-only changes did not address that prepared-plan distinction.

The corrected query uses disjoint list/detail `UNION ALL` branches. The list branch has a one-time parameter guard, so it retains the unfiltered relation's plan instead of a selective optional-ID predicate. The detail branch uses the primary-key equality. No identifier sorting is added, and only one branch can return rows. The regression checks the entire original list body and all three detail bodies repeatedly under forced custom, forced generic, and automatic planning.

This establishes compatibility for the captured database/planner fixture. Neither implementation declares a stable SQL ordering across all PostgreSQL statistics, versions, and data distributions. Other optional-ID provider queries require their own source witnesses before any changes. This is not a performance measurement or acceptance of HTTP permissions, rendering, resource bounds, or the complete EPG job lifecycle.
