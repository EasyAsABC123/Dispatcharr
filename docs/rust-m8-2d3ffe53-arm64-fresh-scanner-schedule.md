# Fresh ARM64 scanner startup and backup schedule on 2d3ffe53

A prior Rust run queued the scheduled scanner with two directories
missing. The scanner faulted and the scheduler released its lock.
The combined Rust candidate created the missing
directories during ordinary Main startup and kept them owned by the
service user. The fresh run observed two real scanner results and
continued to the signed backup workflow.

The same ordinary package verified two restore refusals, one unforced
scheduled backup and retention from five archives to two. The schedule
state remained readable after Main exited. Exact cleanup covered six
containers, two volumes and the private network. The first fixture
attempt stopped before Main because its synthetic database name did
not match the seed guard. The corrected repeat passed independently.

The test covers copied PostgreSQL data. Full M8 and SQLite upgrade
qualification remain open. Original Dispatcharr and its upstream
contributors retain full credit.

[Machine-readable fresh-volume qualification](../scripts/rust-integration/m8-2d3ffe53-arm64-fresh-scanner-schedule.json)
