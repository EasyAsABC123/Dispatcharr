# Bounded configured recording components — authentication-projected publication2

Original Dispatcharr and dependency authors retain credit for the application, algorithms and protocols. This packet qualifies inert library components for the captured UTC Series and recurring-rule subset. It does not enable Main, default FIFO intake, child consumption, recording media, or the full M6 milestone.

Publication1 remains a private blocked artifact because its raw PG transcripts contained SCRAM authentication exchanges. This revision changes publication privacy only:376 authentication payload occurrences in seven proof JSON files are replaced by exact byte length/SHA receipts. Frontend PasswordMessage(p) and backend SASL challenge/verifier(R codes11/12) bytes are unavailable. Every frame direction/kind/order/epoch and every application SQL/query/row/effect/metadata/timestamp remain unchanged, independently walked against private originals. No authentication-byte replay is qualified.

`summary.json` states each actual scope, historical binary and negative. Source11's host exit is **1**, with an independently reviewed observed prefix of 11 cases/103 checks. Its SQLSTATE remains raw `null`; the owned trigger declaration and exact exception/message/CONTEXT establish the narrow fixture origin, not a measured Source SQLSTATE. Normal2 passed 7/9. Known9 passed 9/11, including the two Source-caught Recording INSERT failures. The latter uses an opaque typed error from an actual settled INSERT/rollback/Ready/close; other statements/errors remain operational. No Python frames or exception imports are fabricated.

The physical evidence is deliberately split. The first failed attempt contains 15 passing cache ownership checks; a later failed attempt contains three passing PG phases/43 checks; artwork1 has 12 passing physical checks followed by a false normal-result assertion. It observed one published artwork child and no terminal result after a 6.2-second held reply exceeding the normal 2-second logical deadline. The aborted outer caller did not expose the actual inner error. The separate remaining-seven run passed 101 checks; it does not waive or replay artwork1. Its old QA9 INSERT-error expectation remains operational, separately from Source10's caught-normal semantics.

All raw row/cache/event/result/publication fields and timestamps are retained. `fixtures/*/tools/normal_contract.py` compares field presence, Source changed/unchanged relations, and Native actual command windows. Only declared integral input-date correspondence and ordered one-to-one generated identities are mapped. Historical `default11` labels undercount the real audit: 62 tables, 59 sequences, 501 column rows, 198 constraints and 255 indexes. Claims concern captured manifest-file equality, not physical PostgreSQL pages.

## Exact historical reconstruction

The 24 content-addressed `historical-blobs` plus immutable Git base `044f3ad0fb3ee68ffb17b23bbb796ccdae954298` reconstruct both executed source maps. These are distinct from the proposed current-797 graduation. There is no private source fallback, implicit HEAD or live worktree input.

```sh
TOOLS="$PWD/scripts/rust-job-contracts/configured-recordings"
python "$TOOLS/reconstruct.py" --repo "$PWD" --variant source10 \
  --output /tmp/configured-source10 --record /tmp/configured-source10.json
python "$TOOLS/reconstruct.py" --repo "$PWD" --variant qa9 \
  --output /tmp/configured-qa9 --record /tmp/configured-qa9.json
python -m unittest discover -s "$TOOLS/tests"
```

Both variants have actual local Git-only reconstruction controls; no new application run is implied. `source10` is 802 Rust + five coupled inputs; `qa9` is 801 Rust + five. The historical executed ELFs and full build receipts remain in `proofs`; ELFs are not committed. Rebuild with pinned Rust1.90/offline cached dependencies and a **fresh target on a filesystem with space**, not a copied target:

```sh
# First provision/source your explicitly reviewed Rust/Docker tooling environment.
python "$TOOLS/build.py" --variant source10 --source /tmp/configured-source10 \
  --target "$PWD/artifacts/rust/target-configured-public-source10-1" \
  --output "$PWD/artifacts/rust/configured-public-source10-build-1"
```

`build.py` records actual cargo no-run JSON artifacts, compiler, source map, all waits, and copies job/store ELF peers outside targets only after both waits0. Its public rebuild is **UNEXECUTED at this publication preparation**. Rebuilt ELF equality with historical hashes is not assumed.

## Portable component rerun preparation

Restore the exact committed support snapshot explicitly; an ignored local `scripts/rust-core-perf/support` directory is not a prerequisite:

```sh
python scripts/rust-integration/image54-checkpoint/reconstruct.py \
  --kind support --repo "$PWD" --output /tmp/configured-support-f0
```

`make_inputs.py` accepts explicit support, environment, Compose executable, image-inspect receipt, rebuilt source and build record. It hashes the complete required support/tool closure and emits **unauthorized review-only inputs**, never dispatches a fixture. The image is pinned Original `sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f`; actual run records bind the Original Git archive `1241b3f7...`/`f2323ac7...`. The test peer runs in that pinned image with owned copied SQL/Redis8/9, one CPU/1GiB/pids256 and unchanged limits.

```sh
python "$TOOLS/make_inputs.py" --kind known9 --source /tmp/configured-source10 \
  --build-record "$PWD/artifacts/rust/configured-public-source10-build-1/build-record.json" \
  --image-inspect-receipt /absolute/owned-original-image-receipt.json \
  --environment-script /absolute/tooling/environment.sh \
  --compose-executable /absolute/docker-compose --support-root /tmp/configured-support-f0 \
  --output "$PWD/artifacts/rust/configured-public-known9-1" \
  --inputs "$PWD/artifacts/rust/configured-public-known9-inputs.json"
```

After separate review/owned-fixture authorization, use the printed exact `run_pair.py` command. For Normal7 choose `--kind normal7` and the QA9 source/build. Neither public-wrapper rebuild nor its newly rendered fixture command has been executed merely by preparing this packet. Existing executed controllers are byte-preserved, including historical absolute paths in their evidence; these are provenance, not portable prerequisites.

## Integration and remaining work

The graduation overlay contains 13 selected paths onto Git `9347ba21`: ten exact Source10 files plus three shared registry/transport compositions preserving credentials/M3U/backups. Its full 806 Rust + five coupled map is `4b0c34...`; actual fmt, non-test workspace, strict all-target workspace, defaults611/712ignored and focused18 passed. The host script then failed in a copy-only glob because two feature-specific library ELFs existed. A separate copy-only proof selects the exact tested paths from retained `Running unittests` logs; the host1 is preserved. The separately reviewed metadata-only Known9 rebind then actually passed9/11 on the new `f792d585...` ELF, with165 complete temporal records and five fresh exact project/CID cleanup queries. It does not transfer historical outcomes by assumption.

Open domains include parented configured rules, default graph/family readiness, clocked dispatch and real recording/artwork descendants, broad rule/title/offset/calendar/timezone/equal-time semantics, error-specific Source10 physical gates, the >120-second Source renewer and raw token differences. No resource measurements or gains are claimed here.

## Read projected numeric evidence without a fixture

`authentication-projection.json` binds each private original report hash to its one accepted projected hash and lists all376 payload receipts. Private raw hashes are provenance, **never accepted alternative inputs**. `projected-proof-map.json` binds every file of the projected evidence tree; replacing a projected file with its raw original is rejected even if its application fields remain otherwise usable. No generic raw-or-projected fallback exists.

The54 larger files are deterministically gzip-compressed **after projection**. `proof-compression.json` binds compressed bytes to projected plaintext bytes, not lossless original authentication transcripts. Materialize locally and rerun the portable offline application comparison:

```sh
python "$TOOLS/materialize_evidence.py" --output /tmp/configured-projected-proofs \
  --record /tmp/configured-projected-proof-receipt.json
python "$TOOLS/review.py" --evidence /tmp/configured-projected-proofs \
  --record /tmp/configured-projected-comparison.json
```

Actual local preparation restored all54 projected files (42,020,480 projected bytes) and validated the complete1,319-file projected tree. Application comparisons passed Normal2's123, historicalKnown9's165 and mergedKnown9's165 temporal records plus remaining7/101. All24 local positive/negative controls passed. Public fresh rebuild/input-generation/app wrappers remain UNEXECUTED; no fixtures were rerun for this publication-only correction.

The unchanged historical normal comparator decodes all frontend frame payloads before checking Parse/Query kind. Its first projected-data replay therefore failed on redacted `None`; that offline TypeError is retained. The new public adapter omits only redacted authentication frames from a **SQL-derivation clone**, never from stored evidence or application row/clock comparisons. Original projected records retain all frame epochs, order and receipts. Authentication bytes remain explicitly unqualified; no empty/fabricated authentication payload is substituted.

An independent implementation walks original/private versus projected JSON recursively, checking exact non-authentication keys, types, values, array order and application bytes. Privacy inspection separately decoded35,160 base64/hex fields into538 unique payloads, with zero decode failures or remaining client-final/server-challenge/verifier bytes. Benign authentication mechanism-list/AuthOK messages are retained. Actual EPG password fields are null; all Docker credential variables and Redis ACL password hashes correspond to the already public `playback-fixture-only` fixture literal. No nonempty actual API-key/Authorization/credential URL fields were found. Static source templates/test names and disposable task/lease IDs are classified separately, not silently treated as account credentials.

Executed collector scripts and their historical hash receipts remain unchanged. Fresh collector runs still produce **private raw evidence** and require this separate reviewed publication projection before distribution. `historical-publication1/` metadata documents the old private raw restoration/reviewer attempts; it does not qualify publication1 or authentication replay.
