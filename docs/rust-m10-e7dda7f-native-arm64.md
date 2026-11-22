# Native ARM64 package and startup checks on e7dda7f

Lappy compiled and verified the ordinary ARM64 package for `e7dda7f` using the published source. All 2,159 source input hashes and the 497 protected React, license, and attribution inputs matched. Both native linkage checks passed. The package image was `sha256:2a0d4807711a9443e46443d40b7fbad6d00f97e15ff76158b2e6154621e2fb92`.

A fresh Compose project booted the package as UID 65532 on ARM64. React served the application shell. Initial setup, first-admin creation, JWT login, and a protected channel-group read passed. PostgreSQL contained the expected 184 migration records. The app exited normally, and the observer verified exact retirement of five containers and absence of the owned networks and volumes.

The [machine-readable record](../scripts/rust-integration/m10-e7dda7f-native-arm64.json) binds the exact application, image, and receipt hashes. This verifies native packaging and startup. Controlled rollout and release-wide verification remain open. Original authors retain full credit. [Upstream attribution](../ATTRIBUTION.md)
