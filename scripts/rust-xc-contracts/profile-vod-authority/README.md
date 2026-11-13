# Source profile arguments and cached XC VOD authority

This packet retains 100 literal Source observations across three separate owned fixtures, plus seven qualified Native component tests from an overall-failed fixture. It does not combine those attempts into a successful 100-case run or claim complete M3, M4, M7, Main, process-delivery or performance acceptance.

| Attempt | Actual controller exit | Retained authority |
| --- | ---: | --- |
| Source 1 | 1 | 24 model/SQL argument observations: 21 returned argv arrays and 3 `ValueError` exceptions. The driver metadata phase then failed. The owned Redis graceful-stop failure is retained. |
| Source 2 | 1 | 38 Factory calls: 32 responses with status 200, 2 with status 401, and 4 `Http404` exceptions; 92 actual SQL calls. Its first TCP request was issued, but cleanup's missing `reader` import prevented a complete retained TCP case. |
| Source 3 | 0 | 38 real TCP cases: 32 HTTP 200, 2 HTTP 401 and 4 HTTP 404. All actual body hashes, per-case SQL/sequence equality, deadlines, FD/inode closure and timer joins are retained. No argv or Factory replay. |
| Native 1 | 1 | Before Docker creation or Native tests: the correct Original archive was retained, but a file-backed collector's empty in-memory stdout buffer was returned to the archive guard. |
| Native 2 | 1 | All 7 selected Native physical tests exited 0. SQL, processes, pipes and owned cleanup settled. The final origin-journal audit failed; its DNS-unavailable result remains an overall fixture failure. |

The Source Factory and TCP fixtures have different nonce-dependent names, clocks and cache epochs. Their bodies are not normalized or claimed byte-identical across epochs. The first Source prefix proves literal argument returns/errors and SQL reloads, not complete shlex grammar, driver metadata from that first phase, command execution, HLS or media delivery.

The Native tests cover actual PG work past the unchanged 15-second observer, caller-task cancellation, a retained response frame through the unchanged five-second shutdown observer, existing live/EPG ownership paths, and real late auth-cache replies under a 6,200 ms pause of only the owned Redis. Caller loss is a Rust observer-task cancellation witness, not a packaged Main TCP disconnect proof. Each exact transcript, filter, selected-test count, SQL manifest and process identity/terminal receipt is checked. The qualified prefix remains bound to the historical compile3 prototype; later DTO revisions inherit none of its execution.

The support Compose file has `baseline depends_on: [origin]`. Native 2's reduced controller referenced `/capture/origin.py` but omitted that file. No origin exit or log was captured. `root-native-initial-description-negative.json` preserves the initial overly strong description, and the corrected review keeps the journal explicitly unmeasured. Do not interpret code-path reasoning as a successful no-contact journal or rerun the seven cases merely to repair that audit.

Run the portable checks from a checkout:

```sh
python3 scripts/rust-xc-contracts/profile-vod-authority/verify.py
python3 -m unittest discover -s scripts/rust-xc-contracts/profile-vod-authority -p "test_*.py"
```

Reconstruct immutable inputs without private fallbacks, binaries, targets, Git writes or application execution:

```sh
python3 scripts/rust-xc-contracts/profile-vod-authority/reconstruct.py --repo . --mode prototype --output artifacts/replayed-xc-prototype
python3 scripts/rust-xc-contracts/profile-vod-authority/reconstruct.py --repo . --mode original --output artifacts/replayed-xc-original
python3 scripts/rust-xc-contracts/profile-vod-authority/reconstruct.py --repo . --mode support --output artifacts/replayed-xc-support
```

The prototype is exactly 1,290 application inputs / 793 Rust-workspace inputs: immutable base `044f3ad0fb3ee68ffb17b23bbb796ccdae954298` plus six hash-bound files. Original and support archives use explicit immutable revisions. Every resulting file, overlay preimage and archive pin is checked. This is an input reconstruction recipe, not authorization to replay historical cues or activate a worker. Any new owned fixture requires its own reviewed controller and fresh once cue.

The exact current Source bodies contain no captured XC credential occurrences or sensitive response headers. Full nonce values, cue/claim/environment fields, user seed rows, secret values, host command argv and binaries are excluded. The fixture XC password derives from its full nonce, so nonce omission is part of credential protection. Opaque SQL hashes and identity tokens retain their exact private-raw bindings; private verification recomputes those selections rather than asserting that omitted raw data is public. This privacy projection is separate from body/status parity and never rewrites the original failed results.

Original Dispatcharr authors retain application, model, algorithm and AGPL credit. `Original-LICENSE`, the pinned Original module references and immutable Git recipes preserve that provenance. CPython shlex behavior is separately scoped; no original application or Python module is replaced in the observed fixtures.
