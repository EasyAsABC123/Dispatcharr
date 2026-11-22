# Selected/Core scheduler ownership and known defaults on c1b52dc8

The ordinary `c1b52dc8` package bundles the six-row known scheduler installer
and startup ordering for its explicit Selected/Core mode. Both receivers
must report readiness before Main publishes startup work. Formatting and
both strict lint configurations passed. The host suite passed 1,585
workspace tests and 1,143 copied-data feature tests. Both changes shared a
single ordinary image build, with 348.0 seconds in the Rust compiler step.

One assigned Main handled two startup roots. The Version request received
HTTP 503 and recorded `SUCCESS/null`. The empty DVR recovery recorded
`SUCCESS/Recovery complete`. The owner published one natural UTC cleanup
root with a 43,200-second expiry. The web-only peer stayed ready after the
owner stopped and produced no duplicate startup or cleanup work. All six
owned roles and their process generations retired. The first wrapper attempt
stopped before Compose because it lacked the private engine environment.

In a separate paired run, both backends preserved the six known schedule rows
and their identities through restart. Each executed real Scanner and Recurring
Maintenance work with the expected managed-file and recording outcomes,
including the Recurring artwork child. Stats stayed disabled. The business
and teardown reducers passed.

A read-only check in that same restart fixture found the next database
compatibility difference. Original's PeriodicTask identity sequence stayed
at 10. Rust's advanced from 10 to 16 while the same six task rows kept their
IDs. The positive workflow result and the separately failing sequence
comparison are both recorded. Review also identified the need to retain
bounded readiness waiting for recoverable blocked queue states.

This result covers explicit role ownership and the exercised default schedule
shapes. Default startup, wider saved schedule options, and comparative
application performance remain open.

[Machine-readable evidence](../scripts/rust-integration/m6-c1b52dc8-selected-core-known-defaults.json)

Original Dispatcharr, Django, Celery and django-celery-beat authors retain
full credit for the application, models, algorithms, and protocols.
