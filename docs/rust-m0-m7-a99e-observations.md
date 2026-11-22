# Original guide workflows and HDHomeRun metadata

The verified `a99e1772` ordinary image matched 28 positive GET requests
per backend across 17 HDHomeRun resolver entries. This included fixed,
profile, and combined output aliases. The paired controller exited 0
with independent cleanup. [Machine-readable evidence](../scripts/rust-integration/m0-m7-a99e-observations.json)

Three Original-only checks establish further behavior for the Rust port.

- **Linked XMLTV schedule.** One saved daily UTC cron published one
  refresh root, fetched one guide, and completed its programme index
  and series rule children. One scheduler run persisted, the queues
  emptied, and cleanup passed.
- **Manual channel creation.** Original returned 201 with a real linked
  XMLTV guide, two ordered streams, and one selected profile. The
  programme task ran, the cache generation advanced once, the next
  XMLTV response contained the new channel, and cleanup passed.
- **Fixed HDHomeRun HEAD and OPTIONS.** The eight anonymous cases covered
  allowed and denied network policy under JSON and forced browser
  negotiation. HEAD kept a zero-byte body with declared lengths of 84
  or 22. JSON OPTIONS returned 221 bytes of endpoint metadata. Forced
  browser OPTIONS returned the same metadata in a 10,642-byte HTML shell
  with runtime cookie behavior. Both controlled Original phases exited 0.

The three Original-only results supply reference behavior. Rust replay
of those scopes remains a separate acceptance step.

A four-case named-profile M3U Compose pair also passed on a99e1772.
Original and Rust matched the GET, empty POST, compact JSON-object POST,
and the ordinary nonempty JSON rejection. The response and policy checks
passed with exact cleanup.

A follow-up Original capture verified the two forced-browser OPTIONS
responses against one redacted HTML shell. Each response was 10,642 bytes
and carried a fresh CSRF mask that matched its cookie. The matching
metadata and separate observed hashes are retained in the evidence.

Original Dispatcharr authors and contributors retain full application credit.
Django, Django REST Framework, Celery and all dependencies retain their
authorship and licenses. [Attribution](../ATTRIBUTION.md)
