# Native ARM64 package and startup on 5fdaaba

Lappy built and verified the ordinary ARM64 package for `5fdaaba` from the published source. All 2,161 application inputs and 497 protected React, license, and credit inputs matched. Both native linkage checks passed. The image is `sha256:6c744530615e6ef96feb43a13fb45ff7e79e6127f8860632a34ea78f7e812eed`.

The owned Compose project verified first-admin setup, JWT login, the protected channel-group read, the React shell, and all 184 expected PostgreSQL migration records. The candidate exited zero. All five owned containers retired and the exact network and volume were absent. The [machine-readable receipt](../scripts/rust-integration/m10-5fdaaba-native-arm64.json) binds the image and actual checks.

This acceptance covers the package and ordinary startup. The ARM64 workload comparison and controlled rollout remain open. Original authors retain full credit. [Upstream attribution](../ATTRIBUTION.md)

## M7 output checks

A fresh Compose project on the same ARM64 package passed its four M3U and XMLTV checks. Both authenticated outputs returned 200 and both requests without credentials matched the expected 401 response digests. The observer verified the scoped output and unchanged SQL. The candidate exited normally and exact project cleanup passed. [Machine-readable output evidence](../scripts/rust-integration/m7-5fdaaba-arm64-outputs.json)
