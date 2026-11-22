# Guide and scheduler Compose checks on ad6babd

The ordinary `ad6babd7641067a49ba33f749b8aa39cba1cbb79` image verified
2,156 application inputs, including 497 protected React and attribution
inputs. Formatting and strict lint passed. Rust tests recorded 1,033
passes, zero failures, and 261 ignored tests. The package bound the
actual AMD64 image and default build features. The earlier `27427edc`
candidate stopped at a single formatting difference before lint or Docker.
The successor changed only whitespace in that file.
[Machine-readable evidence](../scripts/rust-integration/m0-m6-ad6babd-compose.json)

## Saved XC and XMLTV schedules

An active linked hourly XC profile ran one normal refresh under the
explicit Selected/Core scheduler. The parent succeeded, the Profiles
child preceded automatic VOD ingestion, and the VOD child succeeded.
The provider requests, durable row effects, and one-run increment matched
the retained Original workflow. The Native fixture retired all three
owned container generations.

A linked daily UTC XMLTV cron produced one natural refresh, one guide
fetch, and two linked children for the programme index and series rules.
The resulting guide and index matched Original. One scheduler increment
persisted on shutdown, the queues emptied, and exact teardown passed.

## Fixed HDHomeRun HEAD and JSON OPTIONS

Six reviewed requests matched Original wire results, covering HEAD under
JSON and forced browser negotiation and OPTIONS under JSON. Both network
policy states were exercised. HEAD returned an empty entity with declared
length 84 or 22. OPTIONS preserved the exact 221-byte ordered metadata.
The forced-browser OPTIONS HTML shell has separate Original authority.

## Guide channel creation

The first Native run returned 201, linked the real XMLTV guide and the
ordered streams, preserved the selected profile, and settled one
programme task. Its exact cleanup passed. Strict effect comparison
exited 1 because Native advanced the cache twice and the first Original
run advanced it once. The observed programme IDs distinguish the worker
paths around the channel create commit. The natural observations stay
separate from the post-commit comparison.

The controlled Original replay held the owned Celery receiver until the
API commit and then released the pending programme job. It matched the
retained post-commit Native result, including two cache invalidations,
programme replacement, the exact stream order and selected profile,
and the next XMLTV response. The Original replay and reducer exited 0
with exact cleanup. The Native pre-commit path has separate coverage
remaining.

Default scheduler activation, further provider shapes, wider HDHomeRun
methods and release acceptance remain open.

Original Dispatcharr authors and contributors retain full application
credit. Django, Django REST Framework, Celery, django-celery-beat, and
other dependencies retain their authorship and licenses.
[Attribution](../ATTRIBUTION.md)
