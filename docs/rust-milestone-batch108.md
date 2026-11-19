# Candidate108 stock channel guide edits and legacy schedule reads

The original Dispatcharr authors and contributors retain full application,
workflow and algorithm credit. The exact package at
`5a15af2673f2f4c15ab1fd58f3589ccee5ae8917` preserved the 497 React, license
and attribution inputs.

The tested channel guide edit accepts the unchanged React payload for an
auto-synced channel with existing profile memberships. Original and Native
passed all 18 scoped HTTP cases, including denial controls. The fixture
verified the stored channel override, the owned dummy guide, unchanged
profile membership and expected cache behavior. Both owned backends and
their subprocesses retired. The result covers the selected noncompact
dummy-guide scope.

The independent schedule-read fixture stopped in its Original SQL helper
before any business HTTP call. Its empty JSON read packet was rejected
because the fixture compared its set of keys with an empty dictionary.
The scoped project cleaned up. That attempt leaves malformed persisted
backup schedule behavior unverified.

[Functional receipts](../scripts/rust-integration/milestone-batch108/functional.json)

## Core JSON-suffix routes

The repaired Core package at `b2086fee47894d80a2cdb0fa334568cbc470b85d`
passed the focused Store tests, strict lint and exact Docker build. The
JSON aliases reuse the canonical validation and authorization, including the
StreamProfile PUT JSON extractor.

Both backends matched all 78 cells across 13 methods on OutputProfile,
StreamProfile and UserAgent routes. The fixture checked six role and
credential cases per method, selected SQL transitions, and unchanged
state for denied and read-only calls. The controller exited 0 and both
owned projects passed exact cleanup. The published product changes cover
the two Core Store files qualified by this run.

[Core JSON receipt](../scripts/rust-integration/milestone-batch108/core-json.json)
