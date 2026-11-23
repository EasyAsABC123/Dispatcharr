# Signed ARM64 backup and restore for 63ee09fc

The ordinary ARM64 package completed a signed PostgreSQL backup and
restore on a disposable copy. Fresh login and readback verified the
restored user, password hash, and schema. The recording returned the
same bytes after restore, with the expected 206, 404, and 206 responses
before reference removal, after removal, and after restoration.

The physical probe verified all 546 installed PostgreSQL runtime
files, including 26 libraries and 20 owning-package notices. It
verified the three client versions and the libraries each client
resolved. The first two fixture attempts stopped on an AMD64-only
manifest path and an ARM64 loader output format. Both attempts
passed cleanup. The corrected run and independent proof exited zero
and verified exact retirement.

Natural retention and refusal behavior have separate scoped evidence
from AMD64. Full M8 and SQLite upgrade qualification remain open.
Original Dispatcharr and its upstream contributors retain full credit.

[Machine-readable restoration proof](../scripts/rust-integration/m8-63ee09fc-arm64-restore-readback.json)
