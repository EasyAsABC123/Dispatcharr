# Union 791 worker-header component regressions

The exact f0bfc2f200dcfd1f64ae32a9855ea250731ce67f Rust dictionary passed 95 real Redis/media checks across ten unchanged batches: 15, 6, 1, 13, 4, 5, 20, 9, 9 and 13. Eight freshly compiled Root peers were copied after compiler exit 0; every executed ELF and log is hash-bound. The independently inspected two owned containers were absent after cleanup. No named network or volume was created.

This qualifies the worker-header component in the combined union. The fixture uses fake resolution with real Redis, local media and HTTP. It does not establish Main, SQL/Bearer authentication, Source-application, image, performance, full header parity or full M4 acceptance. Current default workers were not activated. Original Dispatcharr application and algorithm credit remains with its original authors and contributors; the repository AGPL LICENSE and ATTRIBUTION apply.

`run_regressions.py` is the exact successful executor, with its explicit binding-only diff from the earlier 789-input runner. `outside-peers.json` preserves historical executable paths/hashes for provenance; the ELFs and raw media are not distributed. Fresh source paths/toolchains can change ELF bytes, so they must not be presented as reproducing the historical binary hashes.

Validate retained evidence without executing an application:

```sh
python3 scripts/rust-vod-contracts/worker-identity/union791/validate.py
python3 -m unittest discover -s scripts/rust-vod-contracts/worker-identity/union791 -p test_packet.py
```

For a fresh replay, explicitly reconstruct committed source and its five compile-time golden inputs, build new peers into a unique target, then run the unchanged ten-batch executor:

```sh
python3 scripts/rust-vod-contracts/worker-identity/union791/reconstruct.py --repo . --output /tmp/union791-source
python3 scripts/rust-vod-contracts/worker-identity/union791/build_peers.py --source /tmp/union791-source --output /tmp/union791-run
python3 /tmp/union791-run/run_regressions.py
```

All outputs must be fresh. `build_peers.py` executes only Cargo compilation, checks actual exit 0 and successful compiler completion, validates exact source before/after, and stages all eight targets. It caps each compiler output at 8 MiB, uses an owned process group and a 600 s compilation deadline. It does not mark the 95 regressions passed. The final executor needs Docker access and the pinned Original image used solely for FFmpeg/media generation and Redis; it invokes no Source application. Its existing media, 600 s per-batch bound and physical assertions are unchanged. A new run produces separate evidence and needs its own review. The public build helper has not executed Cargo during publication preparation.

After a fresh run, preserve source/maps/logs and outside executable hashes before cleaning its completed generated target. Do not share that target with another source path or concurrent Cargo process. Existing accepted image 52 and its measured gains retain their original bindings.
