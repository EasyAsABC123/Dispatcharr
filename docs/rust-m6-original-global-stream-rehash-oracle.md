# Original global Stream Settings rehash

The stock Original AMD64 application at `1241b3f7` passed the focused global Stream Settings rehash. An authenticated administrator changed the default stream matching key from `name,url` to `name` using the ordinary settings PATCH. The initial GET, PATCH, and final GET each returned HTTP 200. The setting persisted while the paused consumer held one naturally published task, two matching provider locks, unchanged stream and link rows, and no result.

The normal Celery worker resumed, claimed that same delivery, and completed four streams with two duplicate merges. The recorded progress events were starting, processing and completed. Owned SQL confirmed new hashes and preserved unaffected account overrides and unrelated state. Redis showed the correlated claim, removal of the delivery from the unacknowledged ledger, one terminal success, and zero queued or reserved work and provider locks at the end. The journal observer settled and the exact fixture resources retired. Run, proof and cleanup all exited 0.

Three earlier attempts stopped at fixture setup or role accounting. The successful run configured the permitted origin lifetime, exposed the immutable scripts through the read-only guest bind, and tracked the live observer through the held queue, resumed worker and retirement. The [machine-readable receipt](../scripts/rust-integration/m6-original-global-stream-rehash-oracle.json) links the business and teardown results by hash.

This establishes the comparator for the focused Native test. The Native runtime, full M6 prerequisite, and performance remain open. The original Dispatcharr authors and contributors retain full credit. See [Attribution](../ATTRIBUTION.md).
