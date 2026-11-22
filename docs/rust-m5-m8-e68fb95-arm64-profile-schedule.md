# ARM64 React profile and backup schedule comparisons

The paired Docker Compose fixtures ran on Lappy against stock Original and the ordinary Rust package for application `e68fb950c7ed4abf2ab58da45104f4e2c5f13937`. The Rust image was `sha256:ba228a4a8638aba8b8df77c5f9ac70626c2d6c05b58b3d4364fca2e5b1586068`. Both tests used fresh synthetic PostgreSQL data and independently verified exact teardown.

## M5 React profile workflow

Real Chromium used the same React assets in both arms. Each arm logged in, edited the owned profile through React, read the changes back, restored the original values, and logged out. The SQL checks passed at each stage, including the three restored profile fields and the expected Original form normalization. Both browser runs and the pair reducer passed. The retry recorded the container admission checkpoints individually and kept the original gates, following an earlier failed Original admission.

## M8 Backup schedule GET

Both backends returned matching typed JSON in two consecutive administrator requests for each of two fixture profiles. The migration-seeded default stayed enabled for daily 03:00 with retention 3. The saved fixture stayed enabled for weekly 17:45 on day 4 with retention 7 and cron `45 17 * * 4`. All four cells exited zero, preserved their scoped PostgreSQL rows, and passed exact cleanup.

Original ran stock Daphne with Redis configured. Rust ran ordinary Main with Redis disabled. Both used the plugin opt-out and ran without a worker or Beat. The result covers GET under these declared roles. Redis-configured startup has separate [role-delta evidence](rust-m8-e68fb95-arm64-startup-role-delta.md). The [machine-readable record](../scripts/rust-integration/m5-m8-e68fb95-arm64-profile-schedule.json) contains the exact scoped receipt hashes and outcomes. No benchmark was taken.
