# Ordinary VOD access-flag Source contracts

Original Dispatcharr authors and dependency contributors retain full application,
algorithm and licensing credit. React and Original application files are unchanged.

Seven existing ordinary GET operations now have partial executable Source
definitions: movie, series and episode lists and details, plus category list.
Four exact named workflows passed in the retained unchanged 3,362-test Original
suite. This reuses those results; it performs no new Source execution and adds no
Native acceptance. The register has 131 partial HTTP Source definitions, up from
124. Its 592 routes, 798 operation identities, 322 unresolved methods, 250 non-HTTP
candidates and zero fully verified operations are unchanged.

The original test file is retained byte for byte from revision
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The validator checks its full class AST,
including fixture setup and helper assertions, the selected method assertions,
actual successful log blocks and the existing full-suite reference. Workflow
assertions remain attached to their whole workflow. The results cover absent
access flags, disabled movie visibility, disabled series/episode visibility and
category filtering. Disabled detail requests assert 404; list success does not
establish enabled-detail success.

The callers use `APIClient.force_authenticate` with an explicitly STANDARD user.
JWT/session backend behavior, real TCP and complete roles are unverified. The
anonymous direct `get_queryset` test supplies no HTTP permission evidence. Unified
content and movie-image operations retain their separate prior assignments;
format aliases and category detail receive no assignment from this cohort.

Public-only checks:

```sh
python3 scripts/rust-contracts/vod_access_contracts.py --repo .
python3 -m unittest discover -s scripts/rust-contracts -p test_vod_access_contracts.py -v
python3 -m unittest discover -s scripts/rust-contracts -p test_register_evidence.py -v
python3 scripts/rust-contracts/register.py --output /tmp/new-contract-register.json
```

Optional `--raw-log PATH` checks the exact historical full-suite log hash and
each selected block. It fails when the supplied log is wrong; the portable check
does not silently substitute ignored artifacts. The fixture in `bindings.json`
gives the exact four labels and disposable Compose runner for an explicit future
replay. No replay is necessary merely to reuse these captured successes.

M0 remains open for the other required executable contracts. M1 retains its M0
dependency; M2–M9 and performance acceptance remain separate.
