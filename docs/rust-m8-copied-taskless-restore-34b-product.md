# Copied restore for an opted-in trusted taskless plugin

The original Dispatcharr authors and contributors retain full application
and workflow credit. PostgreSQL and dependency authors retain credit for
the helper and its libraries. [Attribution](../ATTRIBUTION.md)

The non-diagnostic package matched all 2,028 application files in Git
revision `5377ed3a9f232ac0d06540913bdb58c0cf9cc5c7`. Its exact image is
`sha256:5cbb4bbe9639bedc70dfc0534a01218a8f58664611af1dc87cf4c52fcd2c70de`.

The copied suite exited 0 in all six roles. It verified stock and Native
trusted-taskless restore, an ordinary disabled-plugin archive in both
backends, and two refusals before any database change. The successful
trusted restore used one guarded PostgreSQL 17.11 helper. The signed and
stored results agreed, the same Native Main and plugin host served the
restored data, and all owned helper and fixture roles retired.

The Native fixture used Selected jobs with Beat disabled and
`DISPATCHARR_SKIP_PLUGIN_AUTODISCOVERY=1` to isolate the completed restore
from startup repository refresh. It retained the genuine stock-created
dormant schedules. A separate default-refresh run completed the restore
but exited 1 at shutdown with a PluginRefresh permit held. The specific
refresh operation remains open, along with broader M8 acceptance and
post-apply rollback. The trusted-taskless restore is opt-in.
[Redacted receipt](../scripts/rust-integration/m8-selected-taskless-restore-34b-product.json)
