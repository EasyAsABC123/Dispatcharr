# Native ARM64 first-admin setup and authenticated group read

A fresh private Docker Desktop Compose project used the same retained
`d0a80aa5` ARM64 image as the readiness and React index run. A non-root
`aarch64` guest used the owned internal bridge without publishing a host
port. The probe first reconfirmed HTTP 200 readiness and the exact served
React index bytes.

The setup form reported an eligible empty instance. One ordinary setup POST
created the first administrator. JSON login returned access and refresh
tokens, and an authenticated channel-group GET returned an empty array.
A final setup GET showed setup had closed. All five HTTP operations
returned 200. The guest kept the ephemeral credentials and tokens in memory.

The owned PostgreSQL schema had 62 application tables and 184 unchanged
migration records. The database went from zero users to one active
administrator with a recorded login. Channel-group rows stayed at zero.
All five owned containers retired with empty project inventories. The first
attempt exited 1 before account setup because its SQL used an incorrect
fixture table name. That attempt also retired all five containers. The
corrected retry exited 0 with the same image.

The Source basis is the retained ordinary Original first-admin request
fixture and pinned setup, token and group response contracts. This run
adds native HTTP and PostgreSQL evidence for that bounded first-run flow.
Full browser, ARM64 media and copied-data coverage, release and distribution
remain open.
[Scoped first-run receipts](../scripts/rust-integration/m10-d0a80aa5-native-arm64-first-run-auth.json)

The original Dispatcharr and framework contributors retain full credit.
[Attribution](../ATTRIBUTION.md)
