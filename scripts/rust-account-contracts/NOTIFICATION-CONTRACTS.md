# Notification read and personal dismissal port

Original application credit belongs entirely to Dispatcharr's authors and
contributors (ATTRIBUTION.md). Immutable image11 passed 118 focused and 994 full-regression source HTTP
checks. Image/source/report hashes are bound in
`verified-integration-m5-notifications-image11-1.json` in the integration scripts.
No complete notification subsystem claim is made.

Any active authenticated user, including basic level zero, can list/retrieve,
count and personally dismiss visible notifications under the original UI ACL.
Visibility is active, not expired and admin-only eligibility. Ordering follows
original lexicographic descending priority then descending creation timestamp.
Personal dismissals hide list entries by default but never hide detail lookup.
With `include_dismissed=true`, the source's `unread_count` intentionally equals
the included queryset count. Developer conditions are evaluated across the full
eligible queryset before detail lookup, matching the original error/cache order.

The sole registered source condition is `m3u_epg_network_insecure`, checking
literal comma-separated membership of `0.0.0.0/0` or `::/0`. Unknown names are
skipped, dictionary conditions iterate their keys, and malformed unhashable
condition elements remain errors. No expression evaluation is introduced.
Source checker failures return False. Conditions use the actual Django cache
key `:1:dev_notif_condition_<id>_<user>`, primitive pickle booleans and 300s TTL;
cached False must remain distinct from a miss. The network-settings group also
honors shared `coresettings:group:network_access` data and its version guard;
DB fallback skips fill after a cache outage or concurrent version change.

The restricted protocol4/5 codec accepts JSON primitives, dictionary/list and
memo aliases. It never imports modules or constructs Python objects; executable
opcodes and cycles are rejected. Bytes, graph nodes, opcount, traversal work,
nesting and integer conversion are bounded. Source-captured protocol4 fixtures
and an actual isolated Django↔Rust Redis test establish primitive encoding,
False/miss distinction, TTL and oversized-value rejection. The separate source
HTTP suite resets shared condition and settings caches before each backend to
avoid a Python cache fill masking an unimplemented Rust evaluator. Contrasting
DB/group/condition states and stored dismissal timestamps are explicit probes.
A post-image11 real Redis held-loader version-bump test prevents stale fill,
and original Django decodes the stable filled group. The actual concurrent
PostgreSQL-read invalidation race remains a separate unproven gate.

Dismissal creation preserves per-user uniqueness. Repeated false/null/empty
actions preserve the original action; truthy actions update it while preserving
the original dismissal timestamp. Empty bodies are accepted. Dismiss-all affects
only the authenticated user's visible, not-yet-dismissed rows. Collection-valued
`action_taken` remains outside the image11 proof. The subsequent implementation
uses pinned original Python/Unicode15.1 string coercion and source vectors; its
new collection HTTP cases await another immutable image.
Unusual malformed-body HTML errors are outside the exact JSON claim.

Admin notification creation/update/deletion, developer-notification scheduling,
version synchronization, native event production and websocket delivery remain
separate unported responsibilities. Their admin writes return an explicit501;
non-admin writes retain source permission denial. No job side effect is silently
reported as completed.
