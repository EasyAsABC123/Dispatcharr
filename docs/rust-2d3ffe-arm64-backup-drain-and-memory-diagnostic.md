# ARM64 backup shutdown and Original startup diagnostics

## M8 backup across Main shutdown

On Lappy, the ordinary signed AIO Compose package from `2d3ffe5386a7d09b72cda387d0ec9e4f4ab0abc1` ran a signed backup request against a disposable PostgreSQL project. The test held a named table lock until the backup was blocked in `pg_dump`. After a single TERM, the same Main and child process generations remained live and the dump was still blocked on the same SQL lock. Releasing the lock let the job reach Redis `SUCCESS`, produce an owned two-member ZIP with a PostgreSQL dump, and exit Main with status 0. The independent review confirmed absence of six containers, two volumes and the network. [Redacted proof](../scripts/rust-integration/2d3ffe-arm64-held-backup-drain.json)

The first test stopped before sending TERM. The observer expected `pg_dump` as the process name, while the ARM64 launcher invokes a private loader. The passing retry identified the exact loader and bounded client arguments, then checked the parent and process start time. This receipt covers a single held backup during planned shutdown. The full M8 lifecycle is still open.

## M9 stock Original startup observer

A startup-only Compose run captured all 21 members of the current Original cgroup and all four expected stock role roots. The temporary startup support process had already retired. Its retirement and all five exact final resource absences were verified. The first audit rejected a pre-creation name-absence check because it classified it as a final cleanup reference. A second read-only review validated the pre-creation check separately and passed against the same physical run. [Redacted proof](../scripts/rust-integration/31cc-arm64-original-startup-live-roots.json)

The startup-only run exercised no playback or timed guide request. M9 still needs a qualifying paired memory run. The [earlier Original idle-growth disqualification](rust-m9-31cc8a3f-arm64-original-idle-growth.md) stands.

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
