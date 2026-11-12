# Fixed VOD same-SID body characterization

One Source-first/Native image51 pair executed once and exited0. Two same-content, same-SID real SQL/Bearer HTTP clients continuously drained on each backend. They were the same seeded account/user, not evidence of two people. Account/profile max2 was seeded once before independent DB clones; no cap change, Redis hold, admin stop, counter/status condition or adaptive retry occurred.

Both clients200; both backends recorded TWO overlapping non-range GETs: Range/If-Range empty, upstream200, Content-Length4299455, no Content-Range. Origin body writes were1376256/1245184 bytes per backend, with partial body return counts equal to successful writes. Source's final write errors were ConnectionResetError; Native's were BrokenPipeError. These distinctions are retained, not normalized. Neither response delivered its full declared representation.

Client received byte counts at fixed +5/+15/+30/+35 checkpoints were327680/196608,655360/524288,1114112/1048576,1310720/1179648 on each backend. Retention stopped at1MiB per body while reads continued. Four actual prefixes decoded75frames over3s with audio/video; all decoder pipes/processes settled. Source and Native each used7 origin sockets:5 control GETs and2 media GETs,0 HEAD/transient TCP. Body writes, actual client reads, handler return, request-finalization and physical socket shutdown are separate observations.

## Offline reproduction

```
python scripts/rust-vod-contracts/sharing-characterization/validate.py
python -m unittest discover -s scripts/rust-vod-contracts/sharing-characterization -p 'test_*.py' -v
python scripts/rust-vod-contracts/sharing-characterization/reconstruct.py --output artifacts/rust/sharing-restored-blocked
python scripts/rust-vod-contracts/sharing-characterization/verify_contexts.py --repo .
```

The25 offline tests include per-backend missing decoder/files/client/readers/origin coverage, ID/reference/count/clock/byte-progress/physical-close/Source-signal/default-audit/candidate corruptions, plus exact87 reconstruction. validate.py derives required coverage and results from full retained field projection, then checks agreement with recorded flags. It never uses a prior scenario flag to bypass mandatory evidence. Empty collections do not pass. Source actual SQL/Bearer verification is a raw stored witness; the original user-response body was not retained, so this offline check does not replay SQL/authentication. Decoder counts are retained actual decoder observations; optional verify_projection.py also hashes the actual four prefix files when explicitly supplied. No media decoder/network request is rerun by these commands.

retained.json is the full actual summary/phase/checkpoint/decoder/reader/origin/default-audit projection, omitting only listed client response-header maps. Origin literal headers remain exact. Raw summary/phase/seed/cleanup hashes and non-auth seed values are bound; verify_projection.py optionally verifies exact field selection with explicit private paths and prints no auth. Request Bearer, seed JWT, activation nonce and private origin/provider state are not published. Source response-finalization proves request_finished signaling, not generator-finally success.

reconstruct.py defaults to the exact BLOCKED87 qualified source. executed-historical mode restores the exact activation-only87 bytes without granting permission to execute. No fresh cue is provided. Payload policies/proofs/logs retain historical scope; never rerun the consumed cue or synthesize new times/output. Any future application execution needs a reviewed fresh Root cue and owned quiet authorization.

## Interpretation limits

These were sustained body-bearing non-range requests while both client consumers progressed, rather than a one-byte range/header-only probe classification. Origin request IDs and inbound client roles are kept separate; exact upstream-to-client association stays unresolved. Static pinned Original per-view Session.get and Native per-view send contexts support interpretation, not dynamic identity attribution. Same SID can represent seek/retry, not necessarily multiple viewers/people. No single-upstream, automatic pooling proposal, full M4 counter/timer parity, TS-sharing transfer or performance/resource gains claim follows.

Exact default9-table/sequence audits matched; independent project containers/networks/volumes and all owned processes were absent. Candidate image51/exact772 and original archive/support/credits are bound in provenance.json, pinned context files and Original-LICENSE. Root's independent50 local-controller control log is retained as Root50-controls.log; those are control tests, not50 Source application executions.

## Publication revision2: retained coverage and clock repair

The original113-file packet remains immutable, with its eight peer counterexamples retained separately. This NEW tool-only revision requires the complete nine-table/sequence default-audit schema and finite positive elapsed covering all observed phase timestamps. Origin observation spans must follow their own snapshot; Source request-start signals must fall between each known client request start and header receipt, and finalization signals must follow that client's explicit close and lie within the phase envelope. Exact internal phase-start/end timestamps were not separately retained, so the elapsed check uses observed-span bounds; it does not invent exact endpoints or turn finalization into generator-finally proof.

origin-coverage-authority.json pins THIS once-run raw summary and canonical complete origin terminal/snapshot records, IDs and counters. Its own SHA is embedded in validate.py. It prevents deleting/renumbering observed media requests while repairing counts. This is a retained-trace completeness check, not a policy requiring one upstream per client. The structural origin validator explicitly accepts a valid one-upstream trace; any future trace needs its own reviewed authority/hash. Twenty-five offline tests plus the exact57-case peer replay now reject all eight earlier holes. No raw observations, actual exit0,87 executable inputs or runtime behavior changed, and no application was rerun.
