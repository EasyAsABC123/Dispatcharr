# Signed Standard-account browser pair on d41abbb9 ARM64

Fresh Chromium sessions on Lappy compared unchanged React against the ordinary
ARM64 Rust package of `d41abbb90dec665182f2060804e40a4d522b1743` and Original
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The owner, both application arms,
both browser observers and the independent audit exited 0. Both arms served
the same 112-file React asset manifest.
[Machine-readable evidence](../scripts/rust-integration/m5-d41abbb9-arm64-standard-account-browser-pair.json)

The first administrator completed setup, signed in and created a synthetic
Standard user through the signed account API, receiving HTTP 201. The Standard
user signed in with a password, obtained a signed token pair, and read their
typed Standard profile and owned Channels view with the same session. Both
browsers confirmed role-specific navigation, zero administrator-list requests,
zero application mutations from the Standard user and one signed logout
returning HTTP 200. The final database census matched one administrator and one
Standard user, with migrations and settings unchanged.

The audit verified both Chromium contexts and temporary profiles retired
and each Compose down exited 0. Typed absence checks covered Original's
seven containers, two networks and one volume and Rust's six containers,
two networks and one volume. Both project inventories were empty. The
independent audit invoked no engine commands and returned only bounded
observations, counts and hashes.

This comparison covers ordinary account setup, provisioning, role navigation
and logout. Restart persistence and broader React settings and profile
mutations remain open in M5. The original Dispatcharr authors and contributors
retain full application and React frontend credit.
