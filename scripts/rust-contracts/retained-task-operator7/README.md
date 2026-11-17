# Seven retained Original task/operator assignments

All application, protocol and algorithm credit belongs to the original Dispatcharr and dependency authors; AGPL attribution remains unchanged. Definition slices are byte-preserving excerpts of pinned Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`, parsed as provenance and never imported.

Five exact task candidates receive **31 historical worker observations**: VOD root11, selected matching5, single matching7, station discovery4 and M3U aggregate4. Two management-command candidates receive **17 historical Original subprocess cases**: network reset6 and user-network reset11. No new Source run, Native result or HTTP/format-alias assignment is added. Existing139 partial HTTP assignments and all592/798/250 identities remain unchanged. Non-HTTP partial assignments become15, with235 static candidates remaining; fully_verified staysfalse everywhere.

Owning milestones are explicit: M6 for the five tasks and M8 for the two safe commands. These are work ownership, not milestone acceptance.

The station task is defined in `apps/epg/sd_tasks.py` but explicitly registers the Celery name `apps.epg.tasks.fetch_schedules_direct_stations`; the validator reads the exact decorator instead of guessing the broker name from the file path. Actual broker frames, decoded arguments, root/backend UUIDs, observed result/traceback/date/children, and bounded setup/role/effect authorities are preserved. No received-signal, raw backend wire, ACK, role completeness or queued inline-helper execution is invented. Matching keeps DISABLE_ML_DOWNLOADS/no-cache limits, supplied-vector versus model-inference distinction and actual single-error result versus bulk FAILURE.

`fixture-authorities.json` bundles exact hash-bound existing public Source authorities plus the actual old operator integration, so offline validation requires no ignored artifacts, Docker or application imports. `private-provenance.json` records extraction custody. `operator-Original-source.json` is the actual Source oracle, **not** the public Native comparison report: exact before/after fixture values, stdout/stderr, expected exits1/2 and ordered startup/command Channels effects stay intact. Fixtures use owned copied databases, synthetic local providers and fixed fixture users; no production credentials are included. Actual admin HTTP is the task producer scope; login-flow/full actor matrices remain unqualified. The command process uses its owned fixture database credentials; HTTP roles do not apply to CLI invocation.

Reproduce the retained-data assertions and deterministic register without Source apps:

```sh
python3 -B -m unittest discover -s scripts/rust-contracts -p test_retained_task_operator_contracts.py -v
python3 -B scripts/rust-contracts/register.py --output /tmp/NEW-contract-register.json
cmp docs/rust-contract-register.json /tmp/NEW-contract-register.json
```

These controls cover wrong stable identity/source coordinate/task alias/role, changed arguments/result/failure/fixture, invented received signal/new execution/Native parity, missing authority, UUID mismatch and extra HTTP mapping. During the current resource quiet window only JSON/AST derivation and register generation are performed; test execution and shared SDK integration are deferred to the next authorized validation batch. This is progress on M0 Source definitions, not formal M0/M1 or other milestone acceptance.
