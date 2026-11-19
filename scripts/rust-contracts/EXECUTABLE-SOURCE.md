# Partial executable Source contract assignments

Original Dispatcharr authors retain all application/algorithm credit and licensing.
The current register has 257 partial HTTP assignments and 15 partial model,
task, command or consumer assignments. These preserve the initial and subsequent cohorts
documented below. They do not resolve missing methods, complete endpoint role or
grammar matrices, establish new Native behavior or accept M0/M1.

The [proxy/plugin and playback cohort](proxy-plugin/README.md) adds 19
assignments from 148 existing named successes, with exact setup, helper, actor,
mock, assertion and result definitions. Its [verification record](verified-proxy-plugin-bindings-1.json)
retains host checks, the recovered successful SDK exit and its interrupted log.

The [notification capture assignment](notifications-source/MAPPING.md) adds three
operations from eight retained correct-route framework observations and the closed
29-input setup definition. Forced authentication, the original controller exit 1
and recovered cleanup remain explicit. This capture has its own partial role state;
it does not add a new Source run or Native/TCP/credential verification.

The [retained management/channel-guide cohort](retained-management/README.md) maps
37 already qualified admin HTTP observations to 12 exact operation roots. It adds
seven previously unassigned roots and appends five supplemental captures while
preserving their existing Source setup, assertions and results. Source1
controller exit 1 and its four qualified leaves remain distinct from Source2
exit 0/eight leaves and guide exit 0/25 leaves. No applications were run for
this assignment. This brings the published prior 139 HTTP definitions to 146;
652 operation assignments and 235 non-HTTP assignments remain absent.

The [CoreSettings Source32 assignment](core-settings-source32-assignments/README.md)
adds six exact unformatted definitions from 32 retained real-resolver/APIClient
observations, reaching 152 partial HTTP assignments with 646 still absent. It
reuses the complete executed setup/definition closure and existing privacy-adapted
validator. API-key framework authority, recorder `passed:false`, actual controller
exit 0 and unexecuted public projection stay explicit; no Source API is replayed.

The [retained auth51/plugin35 cohort](retained-auth-plugin/README.md) adds nine
definitions and four supplemental captures, reaching 161 partial HTTP assignments
with 637 still absent. Seventy-two observations attach to 13 operation roots;
fourteen method-refusal observations remain separately recorded without invented
IDs. Actual pinned Django successful URLPattern-chain ordinals qualify the
resolver bindings. The separate offline UID3 component preserves its failed exit 1,
raw `passed:false` and one measured, denied urllib3 import probe at `django_setup`;
no business request or resolver is replayed.

The [retained stored EPG cohort](retained-epg-reads/bindings.json) assigns 101
GET observations to six previously unassigned list and retrieve operations for
EPG sources, EPG data and programmes. Three HEAD observations stay separate from
the 798 declared operation rows. Original resolved each path and rendered its
view response through DRF’s request factory. Source TCP and Native behavior
remain outside this binding. The register now contains 181 partial HTTP
assignments, with 617 still absent.

The [retained Core read cohort](retained-core-read/bindings.json) maps 29
real Original HTTP GET observations to seven existing operations. Original
settings, user-agent, stream-profile and output-profile results preserve their
observed roles, statuses and canonical JSON hashes. This brings the total
to 188 partial HTTP assignments, with 610 unassigned.

The [retained serializer cohort](retained-serializer-null/bindings.json)
maps 96 Original HTTP observations to 23 previously unassigned operations.
Its malformed requests preserve 400, 401, 403 and 404 responses and verify no
mutations across 16 protected model tables. The register contains 211 partial
HTTP assignments, with 587 still unassigned.

The [provider, channel and VOD HTTP binding](retained-provider-channel-vod/bindings.json)
adds 132 Original observations across 14 previously unassigned operations.
It retains eight M3U read operation identities, three declared channel producer
POSTs, and three JSON-suffixed VOD collection GETs. There are now 225 partial
HTTP assignments, with 573 unassigned.

The [HDHomeRun HTTP binding](retained-hdhr/bindings.json) adds 120 Original
observations across 25 existing operation IDs. Its nine declared operations and
16 method-unresolved IDs keep their original declarations. The register now has
250 partial HTTP assignments, with 548 unassigned.

The retained playback and VOD packet adds 54 observed Original responses across
seven existing operation IDs. Six keep their method-unresolved declarations.
The register now has 257 partial assignments and 541 unassigned operations.
See the [playback bindings](retained-playback/bindings.json) for exact scope.

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
The initial cohort discovered68 tests (67 passed, one optional retained-private-raw
test skipped). The current account/settings cohort discovers100 (99 passed, one
optional skip); its32 new controls and typed model stage are described in
[accounts-settings/README.md](accounts-settings/README.md).
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

The subsequent [six-task Source cohort](scanner-tasks/README.md) adds six typed
nonHTTP task assignments from74 retained worker observations. That cohort
core suite discovers122 tests (121 pass, one existing optional skip). Exactly15
already-public fixture/capture dependencies are included in the SDK COPY closure.
The prior100/68 counts retain their historical cohort scopes.

The [Source48 mutation cohort](notification-mutation-assignments.md) adds three partial detail-method definitions: PUT six cases, PATCH 34 and DELETE eight. They share one retained 48-request Original framework capture and 1,449 checks; counts are not multiplied per method. Source2 exits 0; the earlier preflight failure remains separate. All 795 unselected operation dictionaries and previous 117 assignments remain exact. Credentials/TCP, Native, full roles and full endpoints remain outside this assignment.


The [Source53/55 station and aggregate input](station-aggregate-source53-55-bindings.json) adds three partial admin-POST assignments from eight retained natural HTTP cases. Source creation and lineup addition use their existing POST operation IDs. Aggregate refresh preserves the original method-unresolved ID/null method and records POST only as a captured observation. All prior definitions remain intact; no Source replay, Native coverage, role completeness, full milestone or performance acceptance is added.
