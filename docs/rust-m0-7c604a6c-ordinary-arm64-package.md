# Exact 7c604a6c ordinary ARM64 package

Lappy built the exact `7c604a6c77fb791c8d4b3b1e30fc58148875f04e` Rust application for Linux ARM64. All 2,179 application inputs matched the reviewed Git tree and the ordinary AMD64 package payload, including 497 protected React, license and attribution inputs. The image index is `sha256:b30aaebdb0990ee5f69076263b46bb38b43c3043872323e97b4f12d86b04764f`. Its ARM64 manifest is `sha256:c4ac4748e48b4bed46bd3c84278140193a699531c92f32f74ad9b24226956fe0`.

Docker Compose verified readiness, the React entry page, fresh first-admin setup, a signed JWT login, and an authenticated channel-group query. The signed smoke ran on an internal network. The package, build, signed auth, owner and proof phases exited 0. A separate same-host audit verified removal of five exact container IDs, one network by ID and name, one volume and empty scoped inventories. Raw image payloads and fixture credentials stayed on Lappy.

This verifies the ordinary ARM64 package, boot and signed startup surface. Full M0, release acceptance and application-wide performance remain open. The original Dispatcharr authors and contributors retain full credit. [Machine-readable evidence](../scripts/rust-integration/m0-7c604a6c-ordinary-arm64-package.json)
