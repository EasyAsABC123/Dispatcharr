# M10 ARM64 cold-copy return after a Rust write

The exact ordinary ARM64 `d2c5e6113efe947551c17bf877e8c6b2c964190c` package passed a fresh disposable same-schema cold return to pinned Original Dispatcharr on Lappy. All owner, preflight, physical run, fixture and proof exits were 0. A second same-host read-only audit also exited 0. [Machine-readable evidence](../scripts/rust-integration/m10-d2c5e611-arm64-cold-copy-return.json)

One internal ingress completed 14 requests across Original, Native and cold Original. Ten carried signed sessions, including eight that reused the Original-issued session. The ingress drained each route before switching. The sole PATCH was Native's channel rename. It changed one row's name and update timestamp, and the checked schema, sequences and unrelated rows remained exact. The Native writer exited 0 and its database sessions drained before the copy.

The initial logical copies covered 62 tables and 501 columns. One archive of the stopped written data restored into two fresh databases for the independent Original startup control and the cold return. The copied column order, rows and sequences matched the stopped writer before boot. The two Original cold starts had the same four bounded startup timestamp effects. Within each startup comparison, all other fields stayed exact. Cold Original then read the acknowledged rename using the Original-issued signed session. It issued zero reconciliation PATCHes. Schema, rows, sequences and login/audit state matched from cold readiness through the signed read and stop.

The exact cleanup audit checked 16 recorded container IDs and 16 reserved names, the owned network ID and name, and six named volumes against the retained missing-object responses. Compose down exited 0, the labeled project inventories were empty and the temporary Compose file was removed.

This rehearsal covers the exercised same-schema cold return. Mixed-schema recovery, production rollout, performance and full M10 acceptance remain open.

Full credit belongs to the original Dispatcharr authors and contributors and the authors of its dependencies.
