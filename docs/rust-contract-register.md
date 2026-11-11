# Contract work register

All application and algorithm credit remains with the original Dispatcharr authors and contributors. AGPL attribution is unchanged.

[The generated register](rust-contract-register.json) provides stable work IDs for the pinned runtime discovery. It is a mapping backlog, not a new parity pass or a percentage of the application migrated.

| Item | Count | Meaning |
| --- | ---: | --- |
| Resolved Django route entries | 592 | Exact resolver order, raw patterns, names and callbacks retained |
| Routes with unresolved method/action mapping | 322 | Explicit unknown-method items; no GET/POST guess |
| Runtime operation work items | 798 | Includes those 322 placeholders; not 798 verified endpoints |
| OpenAPI operations | 362 | Separate discovery view; do not add to route totals |
| Static non-HTTP candidates | 250 | Models, migrations, tasks, management commands and WebSocket candidates |
| Related scoped-evidence links | 16 | Initial ordinary/category links requiring exact case mapping |
| Operations with partial case bindings | 11 | 403 scoped packaged observations; remaining role/input coverage stays open |
| Fully audited register operation rows | 0 | Register-to-case mapping has not been completed; existing reports retain their actual passes |

Each operation has a contract ID, source handler/action, proposed milestone owner, explicit role-matrix state, fixture/assertion placeholders, coverage state and result. Module-associated test filenames are candidates only. An OpenAPI security declaration describes authentication, not authorization. Format variants and duplicate resolver occurrences keep separate IDs.

The runtime discovery's 58 schema warnings and 66 graceful-fallback errors remain in the input metadata. Missing serializers and unknown methods are not converted into passing coverage. Environment precedence, periodic-trigger/crash behavior, plugin context APIs, admin/CSRF/session workflows and startup/shutdown signals still need separate runtime contracts.

Reproduce from the committed pinned inputs:

```bash
python3 scripts/rust-contracts/register.py
python3 scripts/rust-contracts/register.py --output /tmp/dispatcharr-contract-register.json
cmp docs/rust-contract-register.json /tmp/dispatcharr-contract-register.json
```

The builder validates unique IDs, complete route/operation accounting and the absence of invented passes. Input hashes identify the exact discovery files. The output is deterministic and contains no run timestamp. Do not hand-edit the generated JSON: extend the builder or add a reviewed binding input when mapping actual cases.

The next mapping step is case-level: bind a source/native test and its frozen report hash, method/action, role, setup, assertions and observed result. Keep partial input/role coverage partial. Schema-only and Source-only observations must remain distinct from native and packaged comparisons. Milestones M0–M8 remain in progress; this register does not close M0 or full migration acceptance.


## Runtime handler candidates

[The pinned method inspection](rust-runtime-methods.json) adds static handler candidates for 166 of the 322 method-unresolved routes; 156 still have no such candidate. The 322 original unresolved method/action placeholders remain unchanged. A present handler or default permission class does not prove successful dispatch, authorization, CSRF or any response contract. Dynamic permission-method references are retained for later case mapping.

The inspector walks the exact same 592 routes without instantiating/executing their views. A SQL execution wrapper rejects queries during inspection; the successful probe recorded zero inspection SQL statements and zero inspection view invocations. The owned setup separately performs its usual web readiness request. The fixture was removed, and the original source archive matched the pinned hash.

```bash
source artifacts/rust/tooling/environment.sh
python3 scripts/rust-contracts/run_runtime_methods.py --output artifacts/rust/runtime-method-discovery-new
```

The generator uses the committed observation file as an additional hashed input when present. Raw runtime dispatch candidates remain separate from the original resolved ViewSet action maps. No role or method acceptance is inferred.


## Captured case bindings

The [reviewed binding input](../scripts/rust-contracts/case-bindings.json) maps 154 actual image36 GET comparisons to the exact unified list/detail operation IDs. Each case retains its Source index, path/query, role, status, raw body SHA and fresh-tie-authority flag. The binding fixes the packaged report hash and runtime identity, fixture and assertion scope. Six predeclared fresh Original tie authorities remain explicit; historical order is not substituted for their actual bodies. The generator rejects duplicate IDs, mismatched route/handler/method identity, changed report bytes and a different runtime.

Another 34 passing observations use HEAD/OPTIONS/POST/PUT/PATCH/DELETE methods absent from the original ViewSet discovery map. They remain separate observations; the generator does not invent declared operations. The captured HTML200/native501 mismatch remains excluded. Both mapped operations retain `fully_verified=false`, incomplete role matrices and null whole-operation results. All 592 routes and 798 operation identities are preserved. This closes some case-mapping bookkeeping, not full M0/API acceptance.
