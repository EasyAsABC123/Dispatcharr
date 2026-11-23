# ARM64 cold logical-copy rollback after a Native write

The exact `390ad8aa` ARM64 package passed an isolated, same-schema cold return to the pinned Original source on Lappy. The preflight, physical run, independent reducer and outer owner wrapper each exited 0. The reducer replayed byte-for-byte from the retained local observations. [Sanitized evidence](../scripts/rust-integration/m10-390ad8aa-arm64-cold-copy-return.json)

One isolated ingress served 14 successful requests across Original, Native and cold Original. Ten requests carried signed sessions, including eight that reused the Source-issued session. The route changed only after the previous route drained. The sole business PATCH was a Native rename. It changed one row's name and update timestamp, and the other rows, schema and sequences in the write comparison stayed exact.

The initial Source-to-Native and Original-control restores preserved the same logical schema and data, including 62 tables and 501 columns with the declared column ordinal compactions. After the acknowledged write, the Native writer exited 0 and its SQL sessions drained. One common archive of that stopped copy restored into two independently empty databases, one for cold Original control and one for cold Original return. Both preboot copies matched the stopped writer's raw column order, rows and sequences. Both restores exited 0, and the temporary archive was removed.

Cold Original saw the acknowledged rename through the original signed session, with zero Original reconciliation PATCH requests. The paired cold starts showed four bounded startup timestamp effects, with the other fields exact within each comparison scope. From readiness through the signed read and stop, the cold return preserved schema, rows, sequences and login/audit state. The retained Source, Native and control comparisons also passed.

Before creation, the reserved project resources were absent. After Compose exited 0, direct absence checks covered 16 container IDs, all 16 reserved container names, one owned network and six named volumes. The final project-label inventories were empty.

The rehearsal covers one disposable cold logical copy with the same schema. Mixed-schema rollback, a no-effect claim from preboot through startup, production rollout, full M10 acceptance and performance remain outside this evidence.

Original Dispatcharr, PostgreSQL, Django, Redis and other upstream authors retain full credit. [Attribution](../ATTRIBUTION.md)
