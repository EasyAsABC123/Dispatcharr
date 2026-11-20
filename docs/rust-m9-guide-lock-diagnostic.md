# M9 held Redis read and guide lock diagnostic

The original Dispatcharr, Django, Redis and Celery authors retain full credit.

Candidate109 passed the three directed Redis controls and the 120-second
playback and guide refresh check. After the fixture retired the exact socket,
the baseline's first held read returned a backend error. Candidate109's
first held read returned the extant lock with the same value and expiry.

The uninstrumented five-minute replay still timed out after the first accepted
refresh. The same-image passive replay observed that exact publication, the
worker's broker claim and RPOP, then the guide source counter increment.
It observed owner-checked lock cleanup, counter decrement and deletion, with
no acquisition SET or result write in the complete fenced window. Redis
reported a 300-second idle setting. The trace narrows the failure to the
acquisition boundary. The connection failure still needs a directed replay.

The passive controller exited 1 and all five exact service generations
retired. The trace covers the first refresh after five minutes. Two-hour
qualification and broader release acceptance remain open.

[Redacted directed and passive evidence](../scripts/rust-integration/milestone-batch109/postclaim.json)

## Guide lease recovery

Candidate110 checks the same Redis manager before sending its single guide
lease write. The directed controls retired that exact connection. The prior
worker's first acquisition dropped, while Candidate110 acquired a finite
lease on its first call. Both preserved a foreign lease and issued only one
write when Redis completed SET but its reply was lost. Owner cleanup passed.

The exact packaged worker completed 120 seconds with two guide refreshes
and 360 seconds with the guide refresh at +300 seconds. Both runs kept ten
viewers on one upstream, decoded 20 H.264/AAC clips and verified retirement
of all five owned service generations. The directed controls and both
controller exits were 0. The two-hour qualification has its own receipt.

[Candidate110 directed and playback evidence](../scripts/rust-integration/milestone-batch109/c110-guide-lock.json)

## Two-hour playback and refresh soak

The exact Candidate110 package completed 7,200 active seconds with guide
refresh enabled. Ten viewers shared one upstream connection, encoder, and
provider slot through all twelve scheduled guide refreshes. The independent
checker verified 140 actual audio/video decodes, 1,441 counter samples,
121 complete active resource censuses, and retirement of all five owned
service generations. Median working memory rose 23.4 MiB between
the pre- and post-idle windows, within the 64 MiB allowance. The
controller exited 0.

The result qualifies this one-channel soak with guide refresh enabled.
Full M9 still requires the integrated release matrix and rollback gates.
[Two-hour soak receipt](../scripts/rust-integration/milestone-batch109/c110-soak7200.json)
