# Partial executable Source contract assignments

Original Dispatcharr authors retain all application/algorithm credit and licensing.
This additive layer assigns concrete fixtures/assertions/results to thirteen
existing operation identities. It does not resolve missing methods, complete an
endpoint's role/grammar matrix, establish new Native behavior or accept M0/M1.

`register.py` consumes `executable-source-inputs.json`, verifies each input hash,
validates the declared Source authority and applies exact operation-ID, handler,
pattern, action, declared method and owning-milestone guards. Missing inputs or
conflicts fail closed. The generator populates setup/assertion/result fields and
retains partial coverage. It preserves existing case bindings, unrelated rows,
required roles and every underlying resolver/method structure.

The eleven existing HTTP bindings contain 403 historical case observations:
154 unified, 78 provider actions, 50 cached provider-info and 121 image/cache GETs.
Their Source runtime/fixtures/roles/oracles, exact historical response assertions
and raw hashes are explicit in `executable-source-bindings.json`. This is an
attestation/replay of retained observations, not 403 new requests. The archived
image240 oracle is byte-identical to the actual historical helper; a different
current Source-only helper is not silently substituted for it. Predeclared fresh
same-state tie authorities remain distinct. No payload, method alias or header
normalization changes the historical expectations.

One actual new Original-only fixture qualifies the three provider GET operations:
78 scoped status/static-header/available-wire-integrity assertions over seven
roles, exact restoration of eighteen table fingerprints and independent empty
owned cleanup. The inherited 463 ordinary observations are not newly assigned.
UUIDs, clocks and fixture origin ports differ between fresh fixtures, so old
body hashes are not reused as literal fresh expectations. The old 403 exact
response assertions remain bound to their original authorities. The first private
metadata draft mistakenly called these operations M4; their canonical owners are
M5. That metadata was corrected separately, with no Source request/assertion or
historical result change and no rerun claim.

Root's separate framework-auth layer assigns fifteen initialize-superuser and
three JWT-throttle test methods to two existing METHOD_UNRESOLVED identities.
The unmodified Original source, concrete setup/mocks/assertions and eighteen real
successful log blocks come from the qualified 3,362-test execution. These are
Django APIClient tests, not TCP transcripts or fresh execution. Observed POST/GET
behavior does not resolve a method declaration. The stable AST representation is
explicitly portable between the host and SDK Python versions; the failed first
SDK ast.dump comparison remains historical evidence.

## Reproduce without ignored raw artifacts

Run from the repository root. These commands use committed public metadata only:

```sh
python3 scripts/rust-contracts/register.py --output /tmp/NEW-register.json
python3 -m unittest discover -s scripts/rust-contracts -p test_inventory.py -v
python3 -m unittest discover -s scripts/rust-contracts -p test_register_evidence.py -v
python3 -m unittest discover -s scripts/rust-contracts -p test_source_links.py -v
python3 -m unittest discover -s scripts/rust-contracts -p test_source_contracts.py -v
python3 -m unittest discover -s scripts/rust-contracts -p test_framework_auth_contracts.py -v
```

The selected core three modules include the new Source and framework controls:
68 tests discovered, 67 passed, one optional retained-private-raw test skipped.
Provider-specific nineteen fixture/code/role/response guards run separately:

```sh
python3 scripts/rust-contracts/provider_gets/test_contract.py
```

The actual SDK Python check used the qualified network-disabled Rust SDK image
with a read-only input mount and bounded writable temporary storage. It is an
SDK-Python dependency/behavior check, not another Rust build or application image
acceptance. Every default dependency is under the existing verifier COPY and
Docker-ignore allowlist; the original-Django passive fixture remains separate.

## Explicit live Source fixture commands

These are functional fixtures, never resource/performance measurements. Require
Docker/Compose, pinned Original history/image and new empty outputs. They must not
run during another owner's quiet window. The first entry is the live-qualified
provider assertion path:

```sh
python3 scripts/rust-contracts/provider_gets/run.py --output /absolute/NEW-provider-source
```

The generic Source fixture recipes also have an executable entry:

```sh
python3 scripts/rust-contracts/replay_source_contract.py --fixture cached90 --output /absolute/NEW-cached-source
python3 scripts/rust-contracts/replay_source_contract.py --fixture images240 --output /absolute/NEW-images-source
python3 scripts/rust-contracts/replay_source_contract.py --fixture unified154 --output /absolute/NEW-unified-source
```

Those three new shadow entrypoints were not run as new Source workloads in this
handoff. Their historical cases remain explicitly historical. They copy owned
support, retain the actual existing runner through cleanup and use only the
pinned Original. Images use the exact archived oracle in an explicitly recorded
shadow filename; native/canonical flags are not supplied. Fresh checks parameterize
only current Content-Length and the image helper's own recorded 32-hex filename
nonce; they do not convert old payload hashes to new expectations. Unknown failure,
cancellation and physical-close interleavings of these new entrypoints remain
unqualified. Do not infer new full Source scenario coverage from their presence.

Framework fixture execution is the existing `scripts/rust-baseline/run.py` with
the exact eighteen labels listed in `framework-auth/bindings.json`; no rerun was
needed to reuse independently recorded named successes. External-fixture tests
remain separate from pure SDK tests. Root decides the next contract cohort and
formal milestone status; there is no all-798 completion claim here.
