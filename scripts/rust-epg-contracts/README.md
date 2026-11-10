# EPG API contracts

Original Dispatcharr authors and contributors retain full application credit.
The original models, serializers, permission rules and API views are the oracle.

The first scope is source, EPG-data and stored-program list/detail reads. The
fixture below is a draft until executed against the pinned original image and
compared with native SQL/HTTP behavior. It does not accept search, guide-grid,
current-program fallback, poster downloads, source writes or refresh jobs.

After the original comparisons pass, repeat them against an immutable native
image through Compose. Then exercise unchanged React guide workflows and a
paired full-service guide workload, including playback during guide refresh.
Store and render rows incrementally under explicit concurrent-response and byte
budgets; a bounded parser alone does not prove bounded guide/API memory.

`control_reads.py --output PATH` captures actual original list/detail responses,
guide-window helpers, and owned-file format/extraction behavior in one disposable
Compose project. Add `--native` only after registering the draft row-projection
module: it freezes a dedicated Rust archive/target and compares actual PostgreSQL
rows. That native test deliberately excludes auth, HEAD, renderer negotiation,
origin resolution and HTTP resource claims. Helper oracles remain original-only
until their corresponding native comparisons run.

The draft `epg_reads.rs` streams SQL rows but has no HTTP admission, statement
deadline, oversized-row guard or response-body capacity lease. It must remain
unwired until these are implemented and tested, including held body clones,
caller cancellation while SQL is blocked, and large JSON metadata. Search,
current-program byte-offset fallback, custom/dummy sources and guide-grid access
rules require their own original-runtime fixtures before any full M5/M7 claim.

The populated-source oracle exposed join-plan-dependent list order differences. The row-field component compares only Source-confirmed unordered collections by primary key, preserving both original raw responses and every native order difference. This is not HTTP/UI ordering acceptance; the existing API ordering followup stays open and production queries are not replaced with an invented ID sort.

The prepared-plan cause was isolated separately: the optional-ID predicate changes PostgreSQL generic-plan selectivity and join order. Disjoint list/detail query branches preserve the original list plan without an invented sort. The next strict replay compares both row fields and collection order, without primary-key normalization; the earlier scoped row-only evidence remains historical.

The [strict row/order/format replay](verified-row-order-format-components-6.json) now passes all53 native SQL row and collection-order cases without sorting or order normalization, plus399 format decisions using the supplied Source MIME hints. The104 captured Source HTTP cases are not104 native HTTP passes. Host MIME lookup, extraction, resource-bounded HTTP delivery and application refresh scheduling remain separate gates.

The [stored-programme JSON/HEAD component](PROGRAM-READS.md) now has37 actual HTTP comparisons, four physical-capacity scenarios and retained body ownership. Its explicit HTML/CRUD/poster/grid guards and larger guide workloads remain open. The generic row-projection helper remains HTTP-unwired; the independent programme gateway adds its own SQL, admission, transaction and response bounds.

[Current programmes](CURRENT-PROGRAMMES.md) now have64 native host HTTP comparisons plus retained SQL/response ownership checks. The separate guide grid, current file-index fallback and full input/rendering grammars remain unfinished.
