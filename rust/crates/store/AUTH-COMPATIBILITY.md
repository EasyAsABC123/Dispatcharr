# Authentication compatibility

All original application credit remains with Dispatcharr authors and contributors. See the repository attribution and AGPL license.

The pinned original runs Django 6.0.9 with PBKDF2 SHA256 at 1,200,000 iterations and 128-bit salt entropy. Native login verifies the existing Django credential in a bounded blocking pool, then upgrades a matching legacy PBKDF2 SHA1, different SHA256 work factor, or salt shorter than 22 characters. Django counts salt characters rather than UTF-8 bytes. Both username and password serializer fields trim whitespace in the pinned SimpleJWT version.

Password upgrades happen before the active-account check, matching Django ModelBackend: an inactive account with the correct old credential receives an upgraded hash and still receives 401. A failed credential never changes the stored password. A failed old SHA256 credential consumes the missing work-factor iterations in the same bounded worker. Unknown/unusable hashes consume a preferred dummy hash. Existing input, salt, iteration and worker bounds remain in force.

The native update compares the previously read password in SQL before replacement, avoiding an overwrite of a concurrent password change. The original setter saves unconditionally; this concurrency protection is an explicit implementation difference. Argon2, bcrypt_sha256 and scrypt remain unsupported and fail closed.

`verified-auth-hash-source-1.json` binds six real original/native SQL/login cases to the captured Rust source archive and raw report digest. This host component proof does not establish packaged-image, browser, timing-equality or performance acceptance. Run `scripts/rust-integration/run_auth_compat.py --output <empty evidence directory>` with the documented disposable Docker and Rust environment. The ignored `auth_sql` test requires fixture JSON and a real migrated schema; it never silently substitutes a mock.

The original SimpleJWT configuration disables refresh rotation and does not install the token blacklist application. Its blacklist-after-rotation setting therefore does not establish token revocation. Logout flushes the Django session and records an event; existing JWTs remain valid. Native JWT revocation must not be invented as a parity fix.

Remaining authentication gates include shared login/API-key throttles, complete audit/lockout inbox effects, the other installed password hashers, Django session behavior, malformed trusted-cache values and browser login/refresh/logout workflows. XC failed-auth shared-cache authorization has its own source-bound contracts in `XC-AUTH-CONTRACTS.md`.
