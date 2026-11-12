# Original DVR, SeriesRules and EPG-import Source contracts

Original Dispatcharr authors and contributors retain all application/algorithm credit and upstream licensing. Five new `original/` files preserve exact pinned Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` bytes. The existing channel/guide test and two URL snapshots are reused by exact public hash.

This bounded cohort adds 13 partial register assignments from 55 unique successful historical Original3362 workflows (63 operation/workflow associations). There are 90 **static source request sites**, not measured request counts: 73 APIClient sites and 17 APIRequestFactory sites. Original test classes, setup/teardown, helpers, user creation/level writes, DVR arguments, decorators, mocks, assertions and successful named log blocks remain explicit. No Source workload is rerun merely to acquire labels/counts; no Original application module is imported into the SDK.

The APIClient scope covers recordings list/create/delete/file gates, recurring-rule list/create, SeriesRules read/write/delete and bulk removal, upcoming-recording bulk deletion, comskip configuration, and EPG import triggering. It does not establish real recording playback, all CRUD verbs, aliases, role completeness or TCP responses. The Source `users/me` self-grant test is excluded because that operation already has a separate assignment; two queryset/helper-only DVR tests are excluded rather than attributed to REST.

Stop/extend comprise 15 original workflows with 17 factory sites. Each is joined to its explicit `force_authenticate`, `RecordingViewSet.as_view` action map, helper call chain and matching symbolic PK. These are **framework-only direct view invocations, not HTTP/router dispatch or TCP**. Mocked threads, websocket calls and revoke effects remain mocked. The helper `_make_admin` uses `get_or_create(defaults=ADMIN)`: the create default is retained as 10, while an arbitrary existing row's effective level is not asserted or called anonymous. This conditional fixture is kept distinct from the APIClient actors explicitly created with levels 1 or 10.

DVR helper keyword-only defaults preserve STANDARD=1 and explicit `none`/`view`/`manage` arguments from the pinned access module; the User model's separate omitted-field default remains STREAMER=0. Channel/profile/adult filtering assertions remain within the exact original workflows. Forced authentication bypasses credential verification. The four EPG import tests patch `refresh_epg_data.delay`; they prove only their original status/data, mock-call/force arguments and selected query assertions, **not actual Celery publication or execution**.

Portable public-only checks:

```sh
python3 scripts/rust-contracts/dvr_import_contracts.py
python3 -m unittest discover -s scripts/rust-contracts -p test_dvr_import_contracts.py -v
python3 -m unittest discover -s scripts/rust-contracts -p test_register_evidence.py -v
python3 scripts/rust-contracts/register.py --output /tmp/dvr-import-register.json
cmp docs/rust-contract-register.json /tmp/dvr-import-register.json
```

Optional `--historical-log PATH` verifies all 55 byte ranges against the exact retained full3362 log. Default validation needs no private artifact. The existing `scripts/rust-baseline/run.py` fixture definition and selected labels are included as a separately runnable Original environment recipe; fresh execution is explicitly false here. All new dependencies are under the already-copied `scripts/rust-contracts/` SDK subtree.

All old 55 HTTP assignments and all 250 nonHTTP records, including seven partial assignments, are preserved. Declared methods, IDs, required role matrices and fully-verified flags remain unchanged. Five newly assigned method placeholders remain null. No Native/browser/performance claim or formal M0/M1 closure follows from this cohort.
