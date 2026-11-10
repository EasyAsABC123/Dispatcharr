# Typed, settled refresh exhaustion

Original Dispatcharr authors retain full application and algorithm credit and AGPL licensing. Celery, Django and the other framework authors retain their respective credit and licenses.

## Actual Original contracts

`verified-original-epg-exhaustion-1.json` binds three owned copied-database captures: normal file/parser exhaustion (15 checks), normal and status-P0001 cases (29 checks), and terminal SELECT P0001 (17 checks). Eight actual worker executions are recorded across those captures. They use the pinned Original revision/image, broker DB8 and result DB9, guarded names and unchanged application/helper functions. Default databases remain unchanged and projects are removed.

At retry480, both file-busy and parser-busy paths invalidate the stored index and return SUCCESS/null with no child. Normal status updates write `error` and the exact distinct blocked-for-too-long message; the foreign busy file lock or parser counter remains and the refresh lease is released.

A row-scoped P0001 status-update trigger proves that the guarded status failure still returns normal null, with the old idle/null-message state unchanged. It must not be described as a successful status write. Separately, an owned low-privilege role and query-specific RLS fault target the actual terminal status/name SELECT. A nontransactional sequence proves one actual fault per worker. Source catches it and still returns null with the prior error status/message preserved. No reset-before-try error is characterized by these cases.

## Historical native component52

`verified-source-native-epg-exhaustion-1.json` binds four actual Original producer → native FileJobRunner → normal result writer → Original AsyncResult pairs, 52 checks plus 80 targeted refresh regression checks. It supports ordinary file/parser exhaustion and their proven guarded status-P0001 variants. It deliberately leaves terminal SELECT failures operational; Source17 therefore identifies a known unported boundary for a later component. The P0002 pending-result control is a coverage boundary, not an Original parity pass.

`FileJobRunner::run_report_with_dispatcher` is additive. Existing run/run_with_dispatcher APIs retain their old outcome on accepted paths; old run still returns `Failed` for exhaustion. A fresh local reason belongs to each supervised execution. The report exposes `SettledExhaustion` only after physical reporting, terminal handling and actual Released completion. It distinguishes a saved status from an observed, physically settled caught P0001. It never infers normal authority merely from an execute call that discarded an error.

The fixture worker maps only those explicit reasons/report observations attached to `Failed` into normal null. Other coarse Failed, channel, SQL, runtime, capacity, unknown and unsettled outcomes remain operational. Imported/skipped/deferred/missing-source behavior and immutable/default dispatcher behavior retain their prior supported mappings. No generic exception payload or fabricated Python traceback is written.

The separate physical resource peer covers five scenario groups: concurrent reason isolation with old-API/coarse-reset controls; held reporter plus caller abort; held actual warmed TaskLocks DEL reply plus caller abort; replacement-owner release with no typed result; and an unproved SQLSTATE with no typed result. It confirms the server removed the owned key while the release reply was held, but the request guard, admission and completion remained pending. Aborted waiter returns are explicitly unobserved. The targeted regression also passes three real Redis worker resource tests. Frozen strict all-target ingest/job Clippy and 61 default library units pass; eighteen external library tests are ignored, not counted as passes.

The first native attempt passed all four real worker pairs, then its test sink panicked because it read `data.source_id` instead of the actual Source wire field `data.source`. That failed artifact and cleanup audit remain intact. The final retry changes only this test-observer field; runtime bytes are identical.

## Delivery and remaining authority

The selected change comprises FileJob/report observation/fixtureworker modules and one external test file. It adds no Cargo edge, scheduler, main/default consumer or HTTP202 authority. Source/native task IDs are explicitly separate publications; stored status/message/properties/index and busy-key effects match without normalization. Result timestamps are volatile, IDs must match their own publications and both TTLs are checked within 3590–3600 seconds.

Next gates are actual Source P0002 status counterpart and a native caught terminal-query result with separate stage observations and physical ownership. Unknown/reset-before-try failures must remain distinguishable. Component52 artifacts remain immutable when those boundaries are extended. No performance claim follows from these functional tests.
