# Playlist Refresh All with the Original Celery worker

The original Dispatcharr authors and contributors retain full application,
React and Celery workflow credit. [Attribution](../ATTRIBUTION.md)

Ordinary packaged Rust Main can handle the stock admin Refresh All action when
`RUST_M3U_REFRESH_ALL_OWNER=original-celery` and `CELERY_BROKER_URL` identifies
the retained Original Celery worker's broker. The scoped route is
`POST /api/m3u/refresh/`. Startup checks the explicit owner and the worker
configuration. The selected Native worker route retains its existing mode.

The exact standalone candidate and the integrated image each returned the
stock HTTP 202 response. One Original consumer completed one aggregate job
and two successful children. The two fixture accounts each produced their
expected stream. Existing playlist group memberships remained in place and
Original added its two ordinary Default Group memberships. The worker exited
normally, its process generation and all three owned container generations
retired, and the project inventories were empty in both runs.

The first trial stopped before Rust launched because the copied test seed set
a selected Beat flag. The second trial reached both successful child results.
Its fixture wrote the hash setting without the Original cache invalidation
signal, so the worker retained the empty hash key, and the observer omitted
the Default Group. The passing fixture used the Original settings save,
verified the callable hash setting and checked the expected new memberships.
The two failures and the later passes are recorded separately.

The earlier no-owner comparison returned HTTP 202 on Original and HTTP 404 on
Rust. A standalone ordinary Rust consumer, whole provider graph parity and
equivalent-work performance remain open.
[Validation receipt](../scripts/rust-integration/milestone-batch113/m3u-refresh-all-c125.json)
