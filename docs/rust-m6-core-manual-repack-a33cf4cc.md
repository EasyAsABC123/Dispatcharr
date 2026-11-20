# Manual group repack on packaged Rust Core

The paired Compose workflow passed on `a33cf4cc8af0a7753592fc7cca643a73d758c037`. It used the pinned
Original reference and the exact Rust package, with Selected jobs and Beat
disabled. It exercised the existing administrator route
`POST /api/m3u/accounts/{id}/repack-group/` using the ordinary cache
configuration and the same copied PostgreSQL fixture for both backends.

Both backends returned HTTP 200 and `Allow: POST, OPTIONS` for a compact group
and for a one-shot repack with compact mode disabled. Each response reported two
assignments, one release, and zero failures. The ordinary channel detail reads
then agreed on the raw and effective values.

| Group | First visible | Second visible | Pinned effective | Hidden raw | Manual reservation |
| --- | ---: | ---: | ---: | --- | ---: |
| Compact | 3002 | 3003 | 3001 | null | 3000 |
| One-shot | 3202 | 3203 | 3201 | null | 3200 |

The paired fixture compared full scoped SQL snapshots and observed changes only
to the affected raw channel numbers. That preserved row timestamps, the pinned
overrides, each manual reservation, and unrelated rows. The exact account cache
lock was absent after each request. In each backend the fixture restored the
original raw numbers and verified the complete scoped snapshot. Rust Main exited
with literal 0 after a normal Compose stop, followed by exact teardown of the
four owned role generations and empty container, network and volume inventories.

The build manifest digest is `997dd6bb7331e469fcc4419f8eb8aebafd5347259de7f70ddf7ee204a12775f3`. The independent Original
receipt digest is `3108e89acff1d5e4160b1ea7e5e55b0e6cc31599a7a2b17b3e677aa6ce68f282`. The [paired evidence](../scripts/rust-integration/m6-core-manual-repack-a33cf4cc.json) records the
typed observations, actual exit and cleanup. M6 and the performance campaign
remain open.

The original Dispatcharr authors and contributors retain full credit.
