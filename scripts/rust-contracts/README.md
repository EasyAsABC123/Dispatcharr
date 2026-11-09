# Static migration contract inventory

Generate the source candidate register without installing or importing Django:

```sh
python scripts/rust-contracts/inventory.py --output docs/rust-contract-inventory.json --summary
python -m unittest discover -s scripts/rust-contracts -p 'test_*.py' -v
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
