# Signed backup inventory and download on 1b490f5d

The pinned Original and the ordinary Linux AMD64 Rust package matched status, media, and the selected header contract for all eight requests on three backup routes. The signed Administrator listed the backup twice, issued a download token, and downloaded the archive. The issued token downloaded the same archive. The Standard account received 403 on list and token issuance and 401 on direct download.

Both backends returned the same inventory time. The fixture set the freshly generated backup's mtime to 2026-11-09T12:34:56.123456789 UTC. Both responses reported 2026-11-09T12:34:56.123457+00:00 on both inventory reads. Each backend produced its own genuine PostgreSQL v2 backup ZIP. Both download paths on each arm returned the exact local ZIP bytes, declared length, and attachment disposition. Original's archive was 39,057 bytes and Rust's was 39,069 bytes.

All eight responses on each backend contained `Vary: Accept, origin` and the two Allow methods GET and OPTIONS. Original returned `Allow: GET, OPTIONS` in this run and Rust returned `Allow: OPTIONS, GET`. An earlier Original process emitted the reverse order. Compare Allow by method set for these routes and keep the raw order in evidence.

The package, paired run, reducer, and both teardown operations exited 0. SQL, files, and broker state stayed unchanged during the measured requests. Teardown retired the exact two Original and four Native resource identities with empty project inventories. The two earlier f54e52df attempts remain separate. The first Native inventory body failed after matching wire headers and retained no body fields. The second Original probe stopped at its valid reverse Allow order while Native completed the whole-microsecond fixture.

This covers three ordinary GET routes on one package and architecture. HEAD, OPTIONS responses, restore, performance, and full M0/M8 acceptance remain open. Original Dispatcharr, React, Django, Django REST Framework, and dependency authors retain full credit and licenses. [Sanitized evidence](../scripts/rust-integration/m0-m8-1b490f5d-backup-submicro.json)
