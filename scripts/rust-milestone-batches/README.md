# Whole milestone batches

These adapters group the remaining migration work into one global matrix. The
original Dispatcharr authors retain all application and algorithm credit; see
[`ATTRIBUTION.md`](../../ATTRIBUTION.md) and the original license.

Export the current required cells without starting services:

```sh
python3 -B scripts/rust-milestone-batches/coordinator.py --export
python3 -B -m unittest discover -s scripts/rust-milestone-batches -p test_coordinator.py
```

The export joins M2/M3/M5, M4/M7/M8 and M6. It includes the 28 shared package
cohorts exactly once. Root also retains M0 executable coverage, M1 core acceptance,
datastore compatibility and M9 workflow, two-hour soak, rollback and whole-service
performance gates. Owner matrices retain assertions, Source authority and
implementation gaps. An exported cell is required work, not a passing result.
M6 preserves its ordered context dependencies. External unmet dependencies
remain blocked.

The coordinator runs at most three ordinary fixtures globally. Named shared
resources serialize their users; heavy cells run alone. A failed cell blocks
its descendants while independent cells continue. Uncertain cleanup stops new
dispatch. There are no automatic application retries. The process-level lock
prevents a second coordinator from creating another three-slot pool. Do not run
owner fixtures alongside a coordinator invocation.

A binding may strengthen isolation with `exclusive_heavy: true` or additional
`exclusive_resources`. The scheduler combines these with inventory reservations;
it never downgrades a heavy cohort or removes a declared resource. Replacing a
small HTTP probe with a full media cohort therefore retains the real workload's
exclusive requirement.

An execution binding contains one immutable candidate and a `nodes` map keyed
by exported IDs. The candidate uses the M478 adapter's source-map, committed Git,
archive and actual successful image-build fields. Each node supplies literal
`argv`, `evaluator_argv`, `cleanup_argv`, optional `cwd`/`environment`, positive
`seconds`, `cleanup_seconds`, `output_bytes`, and nonempty `input_sha256` hashes
covering its controller, evaluators and fixture inputs. Bindings come from the
owner adapters' reviewed dispatch specifications. `owner_bindings.m235` contains
the M235 binding object; `owner_bindings.m478` names its checked binding file.
The coordinator invokes owner readiness and checks exact argv/environment and
the common candidate before scheduling; arbitrary node argv cannot bypass a
blocked owner. M6's two currently executable probes remain partial even if an
external evaluator claims a whole-workflow pass. The coordinator does not
replace their semantic assertions or old workload guards. Every new candidate
needs actual production image/UID/frontend/credit verification before dispatch.

```sh
python3 -B scripts/rust-milestone-batches/coordinator.py \
  --bindings /absolute/path/to/current-bindings.json \
  --output /absolute/path/to/fresh-results
```

Qualified Original17 authority is imported through M235's retained-source
binding and recorded as `retained_source_authority`; it satisfies its reference
dependency without starting Source or claiming current Main acceptance. A
Source execution binding must enumerate `new_source_case_ids` and its
`previously_qualified_case_ids` crosswalk; overlapping cases are refused.
Additional retained Source contexts need their owner offline-qualification
binding before scheduling, rather than replaying them to fill the registry.
Package dispatch also requires one hashed `source_stage_audit` file covering
its nested Source stages. A `fresh_required` row must name the actual causal,
fixture or clock dependency that makes historical output unusable; an old
controller invoking Source unconditionally does not establish that need.

The coordinator records the attempt before launching a command, bounds its
runtime/output and always invokes the bound cleanup command. It drains both
pipes, enforces one strict combined file quota, and kills/reaps leftover owned
process-group descendants before releasing resources. Cleanup must emit
JSON with `exact_owned_absence: true`. A semantic evaluator can use the literal
`@EXECUTION@` argument to read the persisted execution and cleanup result. Its
JSON must establish `semantic_assertions_passed`, `full_required_workflow`, the
exact `candidate`, and the exported `entrypoint`. All three commands must also
prove process-group settlement and both pipe EOFs. A zero process exit or a safe
501 probe cannot satisfy that contract by itself. Evaluator and cleanup failures
remain failures. Root/owner preparation can resume a failed harness prerequisite
in a fresh result directory while preserving the original failed attempt.

The adapters still need current production-candidate bindings and complete
workflow implementations. Historical image57 performance and component tests
remain scoped to their own inputs. Functional acceptance precedes a separate,
quiet paired performance phase with equal useful work and the full service
boundary. No new milestone or performance gain is claimed by this registry.
