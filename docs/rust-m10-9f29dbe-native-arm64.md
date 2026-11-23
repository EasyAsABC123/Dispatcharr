# Ordinary native ARM64 package and startup on 9f29dbe

Lappy built the ordinary package from the exact published application `9f29dbedf8a144605dd05038024425d55b079ca0`. Its source tree matched all 2,161 independently frozen files, including 497 protected React, license and attribution inputs. A GitHub DNS failure on Lappy was resolved by transferring the verified published Git objects into a fresh local checkout and comparing the received byte map with the AMD64 source authority.

The package initialized 62 public PostgreSQL tables and 184 applied migrations before reporting readiness. In the isolated ARM64 guest, first-admin setup created one account, a normal login issued signed credentials, and a protected group read returned HTTP 200. Both readiness and the React entry page returned HTTP 200. The fixture stayed within its Docker network and ran as UID 65532. All five owned containers retired, and the exact network and volume were absent after cleanup. The wrapper and application both exited 0.

The [aggregate receipt](../scripts/rust-integration/m10-9f29dbe-native-arm64.json) identifies the exact built image and source authority. This is a native build and one scoped startup/auth workflow. Controlled rollout, the complete release workflow, performance and a default switch remain open.

Original Dispatcharr and dependency contributors retain full credit.
