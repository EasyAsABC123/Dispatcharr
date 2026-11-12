# Original channels/guide Source contracts

All application and algorithm credit belongs to the Original Dispatcharr authors and contributors; upstream licenses remain intact. The eleven files in `original/` are byte-exact snapshots of nine test modules and two URL definitions from Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

This bounded cohort associates 26 exact register operations with 167 unique named successful Original test workflows (177 operation/workflow associations). Their 206 static request sites comprise 129 direct sites and 77 sites reached through one explicitly traced class helper. These are **source coordinates, not a captured live request count**. The complete setup, class/data setup, teardown, helpers, decorators, mocks, assertions and successful named log blocks retain their exact scope. No Original module is imported into the SDK and no new Source workload is executed.

The real historical execution is the previously qualified full3362 Original suite. `observations.json` retains each named successful block and exact full-log byte offsets; its hash is pinned in the validator together with the existing full-suite summary and default-user-level authority. The unchanged Original baseline runner is the separate executable Django fixture. Its per-operation selected labels appear in the binding's fixture definitions, with fresh execution explicitly false. Default core tests replay the public retained assertions; they do not silently install Django or claim a new application run.

Associations cover channel list, summary, IDs, retrieve, PATCH, bulk edit, reorder, assign and from-stream; group list/claim/unclaim; profile create/duplicate/membership; channel streams/stats/by-UUID/numbers; four stream helper actions; and EPG current/grid/search. Five methodless APIView rows remain methodless. No aliases, inferred HEAD/OPTIONS, SeriesRules, helper/queryset/model-only tests, or arbitrary module associations are added.

Every actor is traced to the actual setup/helper/in-method authentication and creation/level writes. Source sites exercise explicit levels 0, 1, 5 and 10 plus one anonymous auth-required site. Level 5 is an explicit integer in an Original fixture, **not an invented enum role**. Missing user levels retain the pinned Original STREAMER=0 default. `force_authenticate` bypasses credential backends; APIClient is not a TCP transcript. Required role matrices stay incomplete. Mocks include the actual Source lookup/clock behaviors; HTTP client `.patch()` is not classified as a mock.

These commands use only committed public dependencies:

```sh
python3 scripts/rust-contracts/channel_guide_contracts.py
python3 -m unittest discover -s scripts/rust-contracts -p test_channel_guide_contracts.py -v
python3 -m unittest discover -s scripts/rust-contracts -p test_register_evidence.py -v
python3 scripts/rust-contracts/register.py --output /tmp/channels-guide-register.json
cmp docs/rust-contract-register.json /tmp/channels-guide-register.json
```

An optional `--historical-log PATH` validates every byte range against the original retained full log without executing Source. The default path does not require private artifacts. All new data is under the existing `scripts/rust-contracts/` SDK COPY/ignore allowance; no new outside data dependency is introduced.

All original IDs, declared methods, previous Source assignments, role requirements and unrelated rows remain unchanged. These partial workflow assertions do not prove endpoint completeness, full write/permission/grammar behavior, Native parity, browser/performance behavior, or formal M0/M1 closure.
