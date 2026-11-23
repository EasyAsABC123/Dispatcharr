# Named outputs, catch-up, XMLTV retry, and DVR drain

The disposable AMD64 Compose scopes used application
`0c5133b62dc9f2df0900f2228e25b75baecb6754` and pinned Original
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

## M0 named output GET and HEAD

Both backends passed eight public requests across the named M3U and XMLTV
handlers with the default and named-profile views. GET bytes matched.
Each HEAD had no payload after its HTTP headers. The four GET audit events
per arm were observed and all other scoped database state stayed unchanged.
All four owned roles and project resources retired. Method register
classification remains open.

## M4 active catch-up

A Standard user on each backend opened the issued archive range and
paused at 12.5 seconds, then resumed at 18.25 seconds. Each position
update returned HTTP 204. The same provider slot carried the matching
3,760,000-byte HTTP 206 range. Each arm decoded 74 video frames and audio,
observed continued media after the pause and resume, and retired the
session and provider slot before exact teardown.

## M4 scheduled recording shutdown

Both backends accepted the regular operator stop and decoded their final
partial recordings. On this candidate, Native then exited 1 while
draining its scheduler. The Native-only replay retained matched work,
showed the explicit scheduler diagnostic, and recorded an exit about
0.446 seconds after Docker stop began. Exact app, observer, network, and
volume retirement still passed. A graceful app exit remains a required
check before closing this workflow.

## M6 XMLTV operator retry

Original and Rust each saw the owned provider return HTTP 503 followed
by HTTP 200 after the ordinary operator retry. The failed attempt created
no children. The recovery produced three fresh linked jobs, the exact
cached XML and public programme, and the expected programme index. Both
arms drained the queues and locks and preserved recordings. Exact teardown
passed.

The original Dispatcharr authors and contributors retain full credit.
See the [attribution](../ATTRIBUTION.md) and [license](../LICENSE).

[Machine-readable evidence](../scripts/rust-integration/m0-m6-0c5133b-output-catchup-retry-and-drain.json)
