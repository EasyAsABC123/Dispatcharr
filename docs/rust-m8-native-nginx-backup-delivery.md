# Native backup delivery through nginx

The pinned Native package at `d482e39c` ran two fresh isolated
Compose profiles against the retained pinned Original stock AIO authority.
Each Native profile executed eight ordinary admin and signed download
requests and four token issuance requests across the same four fixture
filenames. Every issuance returned HTTP 200.

With the startup flag absent, all eight Native downloads returned HTTP 200
with the exact 168-byte ZIP. The accelerated profile set
`USE_NGINX_ACCEL=true` and used a pinned nginx sidecar and read-only
protected alias. Both ASCII downloads returned the exact ZIP. Both
Latin-1 and all four beyond-Latin-1 downloads returned the complete stock
HTML 404. All eight accelerated response observations matched the retained
stock status, body and headers. Both profiles delivered the expected
Django Content-Disposition and kept internal redirect headers off the
client response.

Each profile preserved the archive mount and fixture files. Native Main
and the accelerated profile's nginx sidecar exited 0. The fixture checked
container IDs and process generations independently and found no labeled
container, network or volume resources after teardown.

Attempt 1 exited 1. The direct and accelerated profile exits were `0` and `1`. Its scope and cleanup facts remain in the receipt.

The accelerated attempt stopped before starting the nginx sidecar or sending download requests. Independent inspection verified retirement of all four roles that started. The harness compared the stock image inspect identity with its OCI configuration digest.

Attempt 2 exited 1. The direct and accelerated profile exits were `0` and `1`. Its scope and cleanup facts remain in the receipt.

The accelerated attempt stopped before starting the nginx sidecar or sending download requests. Independent inspection verified retirement of all four roles that started. Unqualified image inspect selected the OCI index instead of the AMD64 manifest.

Attempt 3 exited 1. The direct and accelerated profile exits were `0` and `1`. Its scope and cleanup facts remain in the receipt.

All twelve accelerated statuses matched. The six nginx 404 responses used the version-only footer in the private sidecar. Their body hashes differed only by the Ubuntu build suffix, which added nine bytes in the Original stock response. All five roles retired. The retry used the build-name footer in the sidecar while keeping the exact response comparison.

This result covers the four-filename backup download and issuance
workflow. Original Dispatcharr and Django authors retain full credit.

[Redacted paired receipt](../scripts/rust-integration/milestone-batch113/d482-native-nginx-backup-delivery.json)
