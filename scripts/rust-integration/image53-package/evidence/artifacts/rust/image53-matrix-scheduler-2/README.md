# Image53 finite scheduler — preparation only

Original Dispatcharr and dependency authors retain application/algorithm credit. This private controller dispatches only the exact 28 immutable groups from reviewed plan2 SHA `5b347b1c9f631f719489f4d934048184c5c20fe9a00afc27ce19abea2c7da7ad`. Candidate image/manifest, all 6,148 frozen support files, private support Git identity, argv, cwd, required environment overlays and per-node outputs are bound. No live HEAD/WIP, Main4s, notification/worker-header additional gate, resource experiment or implicit extra node is admitted.

This packet has NOT dispatched a matrix node. Root must review its final execution seal and issue a separate exact-seal clearance after Source48 settles and independent cleanup completes. Source48 business success is deliberately irrelevant: an observed nonzero controller exit is admissible when settled/cleaned. The clearance must bind its real exit and cleanup receipts, contain fresh heavy-idle declarations with a finite validity interval, and be issued after those observations. Actual Docker/build/media idle checks happen only upon an authorized first or solo-heavy dispatch, not during preparation.

The first actual process launch is guide-grid, guarded at real UTC minutes 00–35 before matrix output/fixture creation. The scheduler never changes clocks, waits for a fabricated valid clock, changes workload budgets, retries failed outputs or creates supplementary guide attempts. An invalid pre-dispatch guard is an explicit refusal; Root decides any later authorized action.

There are three lanes. Their slots remain occupied after process exit until a hash-bound semantic/outcome and independent-cleanup review is accepted. `passed:true` cannot mask a nonzero/unknown process exit. There is no invented common report/default-database schema: reviewers bind each actual report and record only observed audit fields/counts/source/candidate scope. Cleanup uncertainty halts all new launches. A clean failed ordinary node can release its lane for independent work, but dependent nodes remain blocked. Interrupted/unknown controllers hard-halt their lane/cohort: both the actual state and sealed controller result must say process_settled=true. A review receipt cannot override false/unknown actual settlement. No automatic recovery/resume/retry exists.

Complete-refresh Original reserves its lane for its immediate Native successor after review. No intervening same-lane job is allowed. Native dispatch validates the actual Source report's UTC `fixture_clock` age 0–900 seconds; it never substitutes a new clock or stale reference. The three fixed heavy nodes run in order `sharedTS18`, `sharedTS6-all-down`, `retention100`, alone, after ordinary work settles. All other Docker/build/media work must be idle before each heavy launch. Main4s and the two new Main gates remain separately blocked.

## Local validation — no workload

```sh
python3 artifacts/rust/image53-matrix-scheduler-2/test_policy.py
python3 artifacts/rust/image53-matrix-scheduler-2/test_dispatch_contract.py
python3 artifacts/rust/image53-matrix-scheduler-2/scheduler.py selfcheck
python3 artifacts/rust/image53-matrix-scheduler-2/scheduler.py status
```

`selfcheck` reads/hashes frozen files and private Git identity only. The local controls use synthetic state/clocks/receipts and no Docker, child process or application. Execution is intentionally separate:

```sh
python3 artifacts/rust/image53-matrix-scheduler-2/scheduler.py next --lane 0
# ONLY after Root reviews this seal AND supplies a fresh authorized clearance:
python3 artifacts/rust/image53-matrix-scheduler-2/scheduler.py execute --name guide-grid-packaged --lane 0 --clearance ROOT_FILE --clearance-sha256 ROOT_SHA
```

A dispatcher owns its child/polling session. It retains raw stdout/stderr, actual PID/exit, started/result receipts and partial failure state. Execute returns the actual child exit. No common success is inferred from `exit0`. After inspection, write a new receipt using `NODE-REVIEW-TEMPLATE.json`, preserve independent query evidence, and call `review --review FILE --review-sha256 SHA`. Only the exact Source53 output (or its own stdout for offline154) can be reviewed. `ROOT-CLEARANCE-TEMPLATE.json` remains unauthorized; it is not permission to run.

All logs, failed outcomes and refused/blocked work remain auditable. State/started receipts are fsynced with atomic replacement; a failed/unknown attempted node is never silently relaunched. The execution seal does not include mutable runtime state, future results or future Root clearances. Agent coordination remains required to avoid unrelated work during solo phases.
