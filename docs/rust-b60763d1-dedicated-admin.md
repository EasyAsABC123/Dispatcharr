# b60763d1 dedicated Django admin session workflow

Set `RUST_ADMIN_BIND=127.0.0.1:5656` to enable the dedicated listener
inside the application container. When the variable is absent, Main
starts with its ordinary public listener. The public `/admin/login/`
route continues to redirect to the React login page. Use the configured
Django key for the installed session database. Keep the dedicated
listener on container loopback.

The AMD64 paired workflow used real staff login and logout in both
backends. It verified current-key session signing, the committed
`last_login`, stable session identity on repeat login, and logout
clearing both the browser cookie and the database session. The signed client also read the
same ChannelGroup through public Main on the local host.
The fixture also checked wrong-origin rejection, the private bind,
and the exact encoded preserved-filter URL. Exact teardown passed.

The initial reducer rejected an inherited scope label. The independent
reducer verified the exact source and image binding, physical
workflow assertions, and cleanup from the retained receipts.

The ARM64 ordinary package passed signed smoke, React/readiness checks,
six third-party notice hashes and file modes, and exact teardown. The
workspace passed 1,767 tests, with zero failures and 822 ignored.
Formatting and strict Clippy passed.

Full admin mutation workflows, legacy password upgrades, signing-key
fallbacks and broader session policy remain open. Original Dispatcharr,
Django, React and the upstream contributors retain full application and
dependency credit.

[AMD64 pairing audit](../scripts/rust-integration/m8-b60763d1-dedicated-admin.json) ·
[ARM64 package audit](../scripts/rust-integration/b60763d1-arm64-ordinary.json)
