# Backup schedule startup observations on ARM64 e68fb95

The original Dispatcharr and Django authors and contributors retain full credit. See [Attribution](../ATTRIBUTION.md).

The Native ARM64 startup diagnostic observed four changes to the copied Beat tables before making any HTTP requests. It verified exact cleanup.

| Table | Before | After |
| --- | ---: | ---: |
| Crontab schedule | 0 | 1 |
| Interval schedule | 1 | 2 |
| Periodic task | 1 | 3 |
| Periodic task marker | 0 | 1 |

The earlier Original schedule GET arm ran Daphne with plugin autodiscovery disabled. It preserved its scoped SQL. Native ran packaged Main with Redis configured, all three worker and Beat overrides set to zero, and plugin autodiscovery enabled. Those startup roles differ. The [typed evidence](../scripts/rust-integration/m8-e68fb95-arm64-startup-role-delta.json) retains both contexts and the Native insert counts. The matched stock uWSGI worker and Native web-role comparison remains open.
