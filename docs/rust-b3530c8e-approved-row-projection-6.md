# B353 release projection after scheduler and browser acceptance

The approved 125-row graph now has 15 exact B353 physical passes, two current scheduler dependency receipts, two admitted datastore authorities, and 106 open rows. The six browser and asset rows each passed execution, evaluation and cleanup. The original browser Stats callback failure and independently recovered scope remain recorded separately.

The current scheduler receipts verify B353 install, first tick, recurring paired work and stale expiry. Install and tick each have a canonical physical execution receipt. Recurring work and expiry satisfy current dependencies as independently replayed receipts. The verifier uses separate exact approval pins for B353 and the earlier F38 replay. It leaves the ordinary application build inputs unchanged.

The separately executed detached copied SQLite upgrade passed its five operator cases and account and channel readback. Its source remained read-only. The [typed readback](rust-b3530c8e-copied-upgrade-readback.json) records scope. The approved datastore plan continues to use PostgreSQL for the service.

The [machine-readable projection](rust-b3530c8e-approved-row-projection-6.json) identifies every credited row and open dependency. The exact B353 release remains on hold.

Original Dispatcharr, React, Django, Celery and all upstream authors and contributors retain full authorship and license credit.
