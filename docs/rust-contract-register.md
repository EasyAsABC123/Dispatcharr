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
