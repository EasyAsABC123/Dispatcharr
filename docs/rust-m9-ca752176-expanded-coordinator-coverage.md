# ca752176 Compose and streaming coverage

Original Dispatcharr authors and contributors retain full application credit.

Eleven of 127 required coordinator nodes passed on exact package `ca752176`.
The six installation, operator, Guide and auth nodes passed in the first
campaign. Main passed 1,024 checks across 18 suites and decoded 75 MPEG-TS
frames. Both browser arms passed playback and Stats, with all 112 shipped
React assets verified. The three shared MPEG-TS workflows passed as well.
The retention check served 100 late reads with a concurrency limit of 32
and opened no additional upstream connection.
Each selected node had successful execution, evaluation, and
independent cleanup. The other 116 required nodes still need fresh proof
on this package.

Separately, 17 stream-core and 16 stream-runtime host tests passed with all
1,510 Rust inputs verified against the same package source map. They exercise
bounded retention, held-frame permits, cancellation, and delivery to 100
readers from concurrent workers. Comparative CPU and memory measurements
remain pending.

The coordinator exported the 127-node inventory and passed all 27 of its
host scheduler and ownership tests against the committed ca752 tool sources.

[Six operator, Guide and auth checks](rust-m9-ca752176-operator-guide-auth.md)
[Exact expanded receipt](../scripts/rust-integration/milestone-batch113/ca752176-expanded-coordinator-coverage.json)
