Candidate26 immutable compatibility source correction

The failed collector assumed the pinned dependency image contained /app/apps.
Actual build26 proved that false. Functional Original fixtures provided /app as
an owned archive mount; the dependency image is not an application checkout.

This four-path proposal is based on canonical936e6557558d5374cc265af7b7d8201290cd8407.
It adds a tracked deterministic library archive under rust/runtime so the existing
Rust-only build context includes its exact bytes without expanding its allowlist.
No external checkout, mutable image tag, workstation /app or application bootstrap
is used. prepare.py derives the archive directly from the qualified original.tar
and verifies every selected input against the retained1211-file Source hash map.
Source archive SHA256: f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff.

The candidate set is apps/, core/, dispatcharr/, version.py and LICENSE:618 files.
The existing reviewed exclusion policy removes290 test/cache/HTTP/admin/server/
management files;328 unmodified library/license files remain. The manifest records
all retained and excluded candidate hashes/reasons. Assets outside that candidate
set are not copied. Deterministic USTAR, sorted names, uid/gid0, empty owner names,
mtime0, read-only modes and gzip mtime0 reproduce the exact604199-byte archive.
Original Dispatcharr authors retain full authorship and their original LICENSE.

The collector validates archive hash, inventory uniqueness, member paths/types,
per-file bounds and exact hashes before writing any application library file.
The Source license and input manifest are retained in shipped notices. It no
longer consults /app. This does not demonstrate enabled plugin compatibility or
prove that retained model/signal/helper code cannot execute application effects;
the separately reviewed execution guards/audit and physical acceptance remain
necessary. The future20-path ownership/audit proposal overlaps this collector:
merge its unrelated guarded-runtime changes while retaining this source input.

Offline review.json exercises the EXACT collector validation block: all328 output
hashes match; archive corruption, path traversal and missing members each fail
before any application directory is written. Regenerating twice reproduced the
same archive/map SHA. No Docker build, Cargo, dependency imports, Django setup or
application runtime was executed by this review. Root owns rebuild and package
acceptance; build26's failed log remains unchanged.
