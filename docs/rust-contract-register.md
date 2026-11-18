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


## Source51 matching actions

The [matching binding input](../scripts/rust-contracts/channel-matching-source51-bindings.json) assigns the 14 captured Source51 admin POST cases to the two existing unformatted matching operation IDs: seven collection and seven detail cases. The [Source51 qualification](rust-channel-matching-source51.md) retains HTTP202 responses, natural task/backend identity, the caught single-versus-bulk SQL failure distinction, assignments, progress messages, cache/manual-group effects and actual child lineage. Mapping reuses that capture; it starts no new Source application.

At the Source51 mapping checkpoint, the register had 136 partial executable Source assignments. All other 796 operation dictionaries, all 592 route identities, 250 non-HTTP entries and 403 existing packaged comparison observations remain unchanged. Both matching rows are Source-only with an incomplete admin-only role matrix; Native acceptance, format variants, OPTIONS/schema, malformed/concurrent input and model/plugin domains remain unqualified. Every fully-verified flag remains false. Original Dispatcharr and dependency authors retain all application and algorithm credit.


## Source53 station and Source55 aggregate actions

The [station/aggregate binding input](../scripts/rust-contracts/station-aggregate-source53-55-bindings.json) attaches eight retained admin POST cases to three existing operation IDs. [Source53](../scripts/rust-integration/channel-stations-source53/README.md) supplies three source-create cases and one lineup-add case, with natural HTTP201/200, actual ETA/immediate task frames, SUCCESS/null results, persisted stations and channel-layer progress. [Source55](../scripts/rust-integration/milestone-batch38/Source55/qualification.json) supplies four collection-refresh cases covering zero active accounts, local STD ingestion and caught XC/default-custom errors with actual active membership, child trail/results, SQL/audit/updates and cleanup. No Source application was replayed for this mapping.

There are now **139 partial executable Source assignments**. The aggregate route retains its existing `METHOD_UNRESOLVED` operation ID and null method/action: the binding records observed POST cases without upgrading runtime method discovery. All 798 operation IDs, 592 route identities, 250 non-HTTP entries, 362 OpenAPI operations, 403 existing packaged comparisons and 795 unrelated operation dictionaries remain unchanged from the preceding register. Prior Source definitions are retained. These are incomplete admin-only Source observations; Native qualification, other roles, format/method/schema variants and full milestones/performance remain separate. Every fully-verified flag remains false.

## Retained task and operator Source assignments

[Seven typed retained Source definitions](../scripts/rust-contracts/retained-task-operator7/README.md)
add five task candidates and two management-command candidates. They reuse31
actual Original worker observations (VOD11, selected matching5, single matching7,
station discovery4, aggregate4) and17 Original operator subprocess cases; no Source
application or command was replayed. Actual expected FAILURE and CLI exits1/2,
partial actor/input domains, absent received signals/raw backend wire and exact
Source fixture effects remain explicit. The station function's `sd_tasks.py`
source identity and explicit `apps.epg.tasks.fetch_schedules_direct_stations`
broker name are kept distinct.

The register retains139 partial HTTP assignments and now has15 partial non-HTTP
assignments;235 non-HTTP candidates remain static. All592 route identities,
798 operation IDs/methods/role fields,362 OpenAPI operations,250 non-HTTP identities
and403 packaged observations remain unchanged. Only the seven exact candidate
rows gain definitions; previous eight definitions and every unrelated row are
unchanged. Fully_verified staysfalse. Contract tooling includes19 prepared
portable corruption controls; their execution and shared SDK integration remain
separate validation steps. Formal M0/M1 and complete backend parity remain open.

## Retained management and guide Source assignments

The [37 retained positive admin observations](../scripts/rust-contracts/retained-management/README.md) add seven partial Source definitions and supplement five existing definitions without replacing their setup, assertions or result. The register now has 146/798 HTTP operation assignments and 15/250 non-HTTP assignments. These are Source assignments, not verified endpoints or a migration completion percentage. The 322 unresolved declaration/method placeholders remain unchanged; observed XML GET requests do not resolve that route’s declared method set. All formal milestones remain open.

## CoreSettings Source32 assignments

The [retained Source32 cohort](../scripts/rust-contracts/core-settings-source32-assignments/README.md) adds six definitions from 32 real-resolver/APIClient observations, reaching 152 partial HTTP assignments; 646 remain absent. Every case binds its actual class/action/URL-name and unformatted path. All preceding assignments, supplemental captures and 792 unrelated operation dictionaries remain unchanged. The full executed setup/definition closure and existing privacy-adapted validator are reused; recorder `passed:false`, actual controller exit 0 and unexecuted public projection stay distinct. Source TCP/JWT, Native, full endpoint/role coverage and performance remain unqualified.
