# Static migration contract inventory

Generate the source candidate register without installing or importing Django:

```sh
python scripts/rust-contracts/inventory.py --output docs/rust-contract-inventory.json --summary
python -m unittest discover -s scripts/rust-contracts -p test_inventory.py -v
python -m unittest discover -s scripts/rust-contracts -p test_register_evidence.py -v
python -m unittest discover -s scripts/rust-contracts -p test_source_links.py -v
```

The checked-in inventory scans `apps/`, `core/`, and `dispatcharr/` and records
source paths/lines and original expressions. It has no timestamp and emits stable
ordering, so regeneration is reviewable. Its source SHA-256 covers the path and
contents of every Python source file in those trees (including test files).
`baseline_ref` identifies the intended Python compatibility revision;
`source_head` and `source_sha256` identify the source actually scanned. A different
baseline label does not check out that revision: pass `--root` to an isolated
checkout to inventory another revision. Syntax failures are reported and cause
a nonzero exit. Test files are listed rather than executed or parsed for routes.

Initial snapshot: **612 source files**, **165 HTTP route candidates**, **1
WebSocket route**, **20 includes**, **33 router registrations**, **70 decorated
router actions**, **51 model candidates**, **145 migration classes**, **47 task
candidates**, **5 management commands**, **76 serializer candidates**, **37
viewset candidates**, **1 consumer candidate**, **33 pattern assemblies**, and
**210 test files**. No parse errors were found.

These are static source candidates, not resolved routes or a compatibility pass.
The same route can appear conditionally or under multiple includes; routers
expand into additional runtime routes. Dynamic arguments, includes, decorators,
and pattern assembly are retained as expressions. Class detection follows source
names and module conventions, so inheritance through indirect/custom bases may
need manual review. Decorated Celery tasks are candidates; scheduled jobs can
also be created through settings, migrations, or other runtime logic.

M0 still requires resolving actual Django routes in the pinned baseline runtime,
expanding inherited/router actions, collecting methods/permissions and response
contracts, and assigning an executable test and owning milestone to every
required behavior. Use source locations to investigate candidates and track
coverage; do not use the counts as a denominator for claiming full parity.

## Scoped case attestations

`register.py` validates committed sanitized case attestations by default, so
registration works without ignored `artifacts/` data in a fresh clone. These
attestations preserve source/candidate bindings, raw capture hashes and the
canonical selected-case projection hash. They contain method/path/role/label,
status, selected response headers and body hashes, not response bodies, SQL,
Authorization inputs or credentials. Attestation validation is not a new HTTP
comparison or private raw replay. Endpoint verification stays incomplete.

Use `--deep-evidence` to additionally revalidate the retained private build,
source/candidate matrices and predeclared fresh authorities. This mode requires
the referenced private artifacts and fails closed when they are absent; it does
not start services or send requests. HEAD/OPTIONS/unsupported-verb observations
remain separate from declared operations, and route aliases are never inferred.

Run the public-only reproduction and nineteen rejection controls on a fresh
clone (no private evidence required):

```sh
python3 -m unittest discover -s scripts/rust-contracts -p test_register_evidence.py -v
```

To also check retained private raw matrices when restored locally:

```sh
DISPATCHARR_CONTRACT_DEEP_EVIDENCE=1 python3 -m unittest discover -s scripts/rust-contracts -p test_register_evidence.py -v
```

## Additive executable Source assignments

[Executable Source definitions](EXECUTABLE-SOURCE.md) now populate thirteen
existing operation rows with concrete fixture/assertion/result assignments: eleven
HTTP captured-case operations and two pinned Original framework-test assignments.
Only three provider operations received a new live Original fixture in this
handoff (78 scoped assertions); the other observations and eighteen framework
methods remain historical. Regeneration consumes the exact additive input manifest.
Stable 592/798/250 identities, 322 unresolved methods and fully_verified=0 remain.
The separate Django/DRF passive-binding fixture needs its supported Original
image/mounts; it is not a portable host/SDK discovery module.

## Additional account/settings Source cohort

[Account/CoreSettings assignments](accounts-settings/README.md) add16 partial HTTP
operation definitions from45 exact Original workflows and one partial CoreSettings
model candidate from35 tests. The register now has29 partial HTTP Source
assignments and1 partial non-HTTP assignment. Stable592/798/250 identities,322
unresolved methods and fully_verified=0 remain unchanged. The80 historical named
successes do not claim new Source execution, Native parity or a settings REST test.
