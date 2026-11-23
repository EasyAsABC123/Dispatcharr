# M10 ARM64 cold-copy return after an acknowledged Rust write

The exact `8fc7eef0a702b57d3c6b20dac171478b38e0196e` Rust ARM64 package passed a disposable, same-schema cold return to pinned Original Dispatcharr on Lappy. Preflight, physical run, independent proof, inner fixture, and outer owner each exited 0. [Sanitized evidence and exact package binding](../scripts/rust-integration/m10-8fc7eef0-arm64-cold-copy-return.json)

One isolated ingress handled 14 successful requests across Original, Rust and cold Original. Ten carried signed sessions, including eight that reused the Original-issued session. The ingress drained each route before switching. The sole business PATCH was a Rust channel rename that changed one row's name and update timestamp. The write comparison verified the remaining rows, schema and sequences unchanged.

The two initial logical copies covered 62 tables and 501 columns. After Rust acknowledged the rename, the writer stopped at exit 0 and its database sessions drained. One archive of the stopped data restored successfully into two freshly created databases, one for the independent Original startup control and one for cold Original return. The copies matched the stopped writer's column order, rows and sequences before boot.

Both cold starts produced the same four bounded startup timestamp effects. Within each comparison, all other fields stayed exact. Cold Original then read the acknowledged rename with the Original-issued signed session and issued zero reconciliation PATCHes. Schema, rows, sequences and login/audit state stayed exact from readiness through the signed read and stop. The retained Source, stopped Rust copy and control state comparisons also passed.

The same-host audit matched 16 container IDs and 16 reserved names, the network ID and name, and six volume names to captured post-down missing-object responses. Compose down exited 0, all project inventories were empty and the temporary Compose file was removed.

This pass covers one same-schema rollback rehearsal. Mixed-schema rollback, production rollout, comparative performance and full M10 acceptance remain open.

Original Dispatcharr, React, PostgreSQL, Django, Redis and dependency authors and contributors retain full credit. [Attribution](../ATTRIBUTION.md)
