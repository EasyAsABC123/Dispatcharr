# Auth Hash and shipped operator CLI evidence on 5fdaaba

Three fixtures ran against the exact ordinary AMD64 package from `5fdaaba33be23b0d3d4677f2df96db05080f10d8` with independent Compose projects and synthetic data. Their application execution, contract evaluation and owned cleanup each exited 0.

| Fixture | Passing checks | Qualification |
| --- | ---: | --- |
| Auth Hash | 34 | Original and Rust account and proxy responses on an owned copied database, including a normal password upgrade and denied sign-in |
| Safe17 | 17 | Shipped network-access reset command behavior and database effects, with Original's Django observer and retained separate command evidence |
| Createsuperuser22 | 22 | Shipped command as UID 65532 with the copied PostgreSQL database and Original's Django observer |

The first batch passed Auth Hash. Its Safe17 and Createsuperuser22 launchers failed before accessing the Docker engine, due to a missing output parent and incorrect fixture allocator import respectively. Cold-launch checks caught both conditions before the second physical attempt. That attempt verified each fixture's exact retirement and observed 29 samples with all three projects running. Its aggregate reducer still exited 1 because the internal createsuperuser project had two mappings declared in its reservation and published none. The aggregate gate requires every configured mapping to be physically observed. A third attempt explicitly cleared the two inherited host mappings and kept the same internal baseline, operator and origin services. It verified all three fixtures, exact teardown, every configured mapping and 29 samples with all three Compose projects running. The complete matrix controller returned its expected partial-run code 2. Cleanup, the scoped reducer and the campaign wrapper all exited 0. The [receipt](../scripts/rust-integration/m9-5fdaaba-auth-operator-evidence.json) preserves all three literal outcomes.

This covers the three selected fixture contracts on the exact package. Safe17's help and stderr compatibility, the rest of the operator CLI, the full M9 matrix and comparative performance remain open.

Original Dispatcharr and dependency contributors retain full credit.
