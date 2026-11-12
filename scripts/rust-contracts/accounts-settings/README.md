# Account and CoreSettings Original test assignments

Original Dispatcharr authors retain application/algorithm credit and licensing.
This cohort reuses **80 exact named successful tests** from the qualified pinned
3,362-test Original execution. It runs no new Source workload or resource sample.

- 45 Django APIClient/RequestFactory workflows support 16 additional existing HTTP
  operation identities: user self-service, per-app storage, user-list/detail,
  group/permission/system-event reads, and locked stream/output-profile edits.
- 35 model/cache/DVR/EPG/serializer workflows support exactly the existing
  CoreSettings model candidate. They do not assign any settings REST route.

`bindings.json` records the exact Original files, stable method/action/handler/
owner coordinates, fixtures, setup/helpers/mocks, actor contexts, request AST
sites, unchanged assertion expressions and successful log blocks. `observations.json`
contains the immutable per-test observations, bound to the actual full-suite log
hash. The Source files are parsed as data; Django is never imported by validators.
The shared portable AST fingerprint helper retains fields/empty lists/typed values
with only its previously qualified empty PEP695 compatibility rule.

## Scope boundaries

Django APIClient is not TCP. APIRequestFactory invokes a view/function directly;
its tests do not prove router dispatch or authentication-backend behavior. Forced
authentication, anonymous calls and the real fixture API-key call remain distinct.
A target user's ID is not treated as the caller. The self-service fixture omits
`user_level`; its actual pinned User default is STREAMER0, not STANDARD1.

URL helpers and resolver-order representatives are statically guarded against the
existing runtime register. A representative integer/key is not claimed as an
observed fixture ID or wire request. Declared METHOD_UNRESOLVED stays unresolved
when tests exercise GET/PUT/DELETE. Format aliases are not assigned. Request sites
inside loops are syntactic locations, not measured request counts.

Assertions belong to the **whole original workflow**. For example, a model delete
following an app-data PUT is not attributed to that PUT endpoint. Global helper
assertions, setup/cache teardown, decorators and mock contexts stay in their exact
source/class fixture context. No response bytes, real TCP transcript or unobserved
permission matrix are fabricated.

Two exclusions are deliberate: notification factory tests use `/api/system/...`
instead of the registered `/api/core/...` path; a locked-profile model-only test
has no HTTP request. Neither becomes HTTP route coverage. CoreSettings direct
model/serializer tests do not become REST/API evidence. The partial model
assignment does not verify every CoreSettings method or Native model behavior.

## Public-only reproduction

From the repository root:

```sh
python3 scripts/rust-contracts/account_settings_contracts.py
python3 -m unittest discover -s scripts/rust-contracts -p test_account_settings_contracts.py -v
python3 scripts/rust-contracts/register.py --output /tmp/NEW-register.json
python3 -m unittest discover -s scripts/rust-contracts -p test_inventory.py -v
python3 -m unittest discover -s scripts/rust-contracts -p test_register_evidence.py -v
python3 -m unittest discover -s scripts/rust-contracts -p test_source_links.py -v
```

No ignored raw artifact is required for these controls. Explicit future Original
execution can use `scripts/rust-baseline/run.py` with the exact labels in each
fixture's `execution.argv`, a fresh output, supported pinned image/Compose and
owned cleanup. No suite rerun was performed merely to reuse actual named successes.

The manifest has one explicitly reviewed account/settings kind, not an arbitrary
module importer. HTTP application and the separately typed non-HTTP application
both reject identity/source conflicts. The latter preserves all 250 candidate
identities, kinds and source descriptors. All endpoint/candidate fully-verified
flags remain false. This cohort increases partial Source assignments only; formal
M0/M1 and every Native milestone remain separate.
