# Native notification mutations: 67 observations, controller failure retained

This is private-source component evidence, not a successful overall fixture or a packaged Main acceptance. The exact frozen source is Git f0bfc2f200dcfd1f64ae32a9855ea250731ce67f plus three selected Rust overlays: notifications.rs, notifications_mutations_sql.rs and notifications_sql.rs. It contains 792 Rust inputs plus five unchanged coupled goldens (797 total), map SHA 46d649d3503e6ee202f1393c3981871fe74da75f4a554ae275247e84915090b9. The selection SHA is b527243fb8bf63a40db0c63eef29969e4ae745009caa938b65727e1b2b9d5a98. No live files, registry, Cargo graph or Main hook changed during curation; accepted image 53 remains its separate f0/791-input build.

Both actual Rust tests exited 0: 48 retained-Source cases plus three explicit Native order controls, and legacy eight Source-derived cases plus eight extras. Independent offline comparison qualifies all 67 requests, own/cross-request clocks, SQL effects, sequences and cache projections. **The controller exited 1 and overall_fixture_passed remains false.** No report, exit, failed command or raw observation was relabeled or rerun.

## The cache-epoch collection negative

The owned Redis is shared by two sequential SQL-target epochs. A successful SET opens mutation DB1; its cache-before, Rust final_cache and primary cache-after all captured DB1 ownership. The second explicit SET opens legacy DB2. The controller's finally later asked its cache collector to capture DB1 while the live sentinel was DB2. The ownership guard correctly refused with ValueError: OwnedRedisMarker (command-025, actual exit 1), leaving no late DB1 cache snapshot. Its exact stderr is retained.

The valid earlier DB1 primary cache-after and inside-Rust final_cache remain authoritative for epoch 1. Their DUMP payloads match exactly, while PTTL observations remain literal, bounded and time-dependent. All 51 mutation/control before/after named H/V/U/D condition projections, sentinel ownership, bool pickle/GET/DUMP/TTL checks and the inside-Rust final named projection are validated. H=False, V=True and D=None behavior is retained; U's captured transition and V's cached value after the selected delete remain literal. No flush, marker rewrite or normalized timestamp is used to manufacture a current DB1 snapshot.

Independent SQL credential audits for DB1 still completed after the transition because their database binding is separate from Redis ownership. The legacy DB2 final cache and credentials completed under DB2. The command timeline retains all 35 physical command receipts, the two successful SET transitions and the one refused cache call. No speculative future collector patch is applied to the executed controller.

## Scope and source authority

The Source 48 authority is the already-published framework-client capture at scripts/rust-contracts/notification-mutations-source, raw SHA 68542935c46471505dfbc9e1442e45fa9cbe498ef2c587fe0c257fb2ebd8b1bd. The packet's sanitized Source projection preserves its statuses, bodies, own clocks, SQL and named cache data; it is not a new Source run or paired credential/TCP proof. The Source renderer/framework exception on its last case is retained. Native's existing JSON 500 response is deliberately different: renderer-body parity is false. Forms/multipart, concurrent physical SQL cancellation and broader grammar/permissions remain open.

The three Native controls preserve poison-before-malformed-PK/body, standard-before-poison, and anonymous-before-poison ordering. The legacy first eight responses/effects are rechecked against Source 8 with explicit own-clock mapping. Its first five extras retain their definitions; only the former last three guarded 501 expectations are replaced by the selected private implementation's PUT-required-fields 400, empty PATCH/update-clock 200, and DELETE/cascade 204. This private component does not alter the already-published image 53 Main's guarded CRUD evidence.

Credential row/event effects and all eleven sequences remain literal except that private password/API-key values are replaced by stable SHA markers in the public projections. Mandatory user/credential coverage prevents empty or dropped fields from qualifying. Business response bytes and hashes, generated clock mappings, cache raw bytes, sentinel strings and SQL notification/dismissal rows remain retained. Secret redaction is not a normalization of clocks, values or mutations. The original credential-bearing raw reports remain private and hash-bound; the packet does NOT advertise byte-exact public gzip copies of those raw files.

Default audits cover exactly eleven named model tables and eleven sequences. Both before/after audits match. Actual controller/native processes settled, the exact pre-cleanup PID/starttime set is absent afterward, all owned project container/network/volume inventories are empty, and the exact CID has a no-such-object receipt. These cleanup facts do not turn controller exit 1 into a pass.

## Build and workspace bindings

All six authorized compile/control commands were waited and exited 0 before the two outside ELF copies. Their historical hashes and copy receipts are retained; binaries and target directories are not distributed. Recompilation at another path/toolchain is not claimed to reproduce an ELF hash or inherit the old SQL execution.

The additive whole-workspace receipt binds the same source map: fmt, non-test build and strict checks exited 0; unchanged host defaults passed after the retained sandbox 101/four ingest-HTTP socket EPERM attempt. Root parsed 584 passed / 0 failed / 706 ignored across 172 result blocks. These are aggregate test outputs, not 584 unique contracts. This receipt never changes the Native67 controller exit or promotes the private runtime to a package.

## Portable offline checks

    PYTHONDONTWRITEBYTECODE=1 python3 verify.py
    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s . -p test_public.py -v
    PYTHONDONTWRITEBYTECODE=1 python3 reconstruct.py --repo /path/to/Dispatcharr --output /tmp/native67-source

The source reconstructor uses only explicit immutable Git f0 blobs, exact preimages, the three included overlays and all 797 hash entries. It neither executes Cargo nor copies targets/ELFs. The five coupled goldens are reproduced from Git and checked. An actual read-only reconstruction was run; no application or workload was executed.

The original 21-input execution seal and controller are retained for provenance. Two ELF inputs, the credential-bearing raw Source48 and the old credential-bearing legacy reference are omitted rather than falsely advertised as fully materialized. Each omitted input's original size/hash remains explicit. The already-public Source48 raw authority remains separately available; a future fixture requires new build/executable/input review and authorization. No consumed execution seal authorizes a rerun.

Optional explicit private comparison, without printing values:

    PYTHONDONTWRITEBYTECODE=1 python3 verify_private.py --repo /path/to/Dispatcharr

Public default verification needs no private fallback. Data-authority hashes bind this exact observation; independent structural validators recompute required coverage, responses/effects, cache epochs, clocks and cleanup first. Corruption controls challenge missing cases/credentials/cache keys, wrong sentinel, dropped transition, changed failure, clock rewind, sequence mutation, missing PID coverage, incomplete build waits and attempted controller promotion.

Initial curation-only failures are recorded separately: a mistaken DUMP footer-length check, wrapper-vs-value default-audit lookup and collector-vs-Rust credential schema lookup. Correcting those readers did not change retained data, workloads, guards, budgets or runtime behavior. The final validators pass the unchanged data.

Original Dispatcharr authors and contributors retain model/serializer/algorithm credit and licensing; see Original-LICENSE. Full M5, Source credential/TCP equality, renderer/forms/physical-cancellation/Main acceptance, default graph activation and performance claims remain false. No Source, SQL, Docker, Cargo, Main, media or benchmark workload was rerun during curation.
