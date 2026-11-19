# Connect paired role matrix and evidence checker

This tooling reconstructs the byte-identical Connect matrix from a full fork
checkout. It reads the pinned Original commit and historical tooling blobs,
applies two exact text overlays, and installs the three new matrix modules.
The materializer reproduces the 23 copied inputs, the authenticated role
matrix plan, and the same fixture seal used by Candidate106. Generated
archives, build manifests, physical reports and runtime data stay outside Git.

The matrix covers 20 exact previously unassigned Connect operations. Ten
canonical methods and ten literal JSON variants each receive anonymous,
inactive administrator JWT, viewer JWT, standard JWT, administrator JWT
and administrator API-key requests. The fixture checks 240 requests using
independent disposable mutation targets and verifies the selected SQL rows,
FK effects, sequences, zero owned deliveries and exact cleanup.

## Prepare offline

Use a full fork checkout that contains the pinned commit history. This step
uses only local Git objects and opens no Docker connection.

```sh
tool=scripts/rust-integration/connect-matrix
fixture=artifacts/rust/connect-matrix-local
python3 -B "$tool/materialize.py" --output "$fixture" --self-test
python3 -B "$tool/requirements.py" --fixture "$fixture"
python3 -B -m unittest discover -s "$tool" -v
```

The output must be a fresh direct child of artifacts/rust because the exact
original launcher resolves its checkout and owned Compose tools relative to
that layout. Use the materializer's self-test entry point to regenerate and
validate the old plan against the current checkout. The checker regenerates
its Source-derived operation requirements from the same pinned authority.

## Run the packaged scope

With the existing owned validation engine, configure its normal socket and
Docker configuration, and supply an immutable passing candidate build manifest
and its full revision. The launcher uses the existing dispatcher lock.

```sh
export DOCKER_HOST=unix:///tmp/dispatcharr-docker.sock
export DOCKER_CONFIG="$PWD/artifacts/rust/tooling/docker-config"
python3 -B "$fixture/run.py" --preflight --build "$build" --revision "$revision"
python3 -B "$fixture/run.py" --build "$build" --revision "$revision" --output "$run"
```

The physical run uses one disposable Compose project. Original and Native
each run both variants with independent owned seed phases. Role and response
mismatches remain in the corresponding cases. The known possible Original
authorized nested subscription JSON 500 remains a failed cell.

## Check actual evidence offline

Supply the actual run directory and the build manifest used for that run.
The checker requires an independently reviewed custody document and its
separately approved SHA-256. It does not manufacture custody from candidate
artifacts or use the fixture-only self-tests as physical evidence.

```sh
python3 -B "$tool/check.py" --fixture "$fixture" --evidence "$run" \
  --build "$build" --custody "$reviewed_custody" \
  --trusted-custody-sha256 "$approved_custody_sha256"
```

A C106 outer paired proof can supply custody when it binds all eight
required files. Older summaries bind only four. In that case the independent
reviewer can provide a separate version 1 custody document with
format_version, scope, matrix_fixture_seal_sha256, project, candidate_revision,
candidate_image, build_sha256, files_sha256 and approved_actual_run.
The scope is inner_owned_Connect_matrix_terminal_and_cleanup. The exact
files_sha256 keys are binding.json, receipt.json, execution.json,
execution.stdout, execution.stderr, independent-cleanup.json,
evidence/integration.json and evidence/owned-before-compose-down.json.
The approval concerns capture custody. A settled, clean run can contain
failed case pairs.

The checker separately revalidates terminal and EOF, project and image,
input hashes, owned generation removal, no foreign SQL writes or deliveries,
and literal method/path/role case matches. It reports representative scopes
and the remaining Source-derived requirements for whole-operation review.
The register stays read-only and its existing acceptance criteria apply.

Dispatcharr's original authors and contributors retain full application
and behavior credit.
