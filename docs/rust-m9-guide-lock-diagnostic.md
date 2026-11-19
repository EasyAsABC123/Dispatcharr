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
