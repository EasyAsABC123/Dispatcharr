# Portable seven-context cohort

This additive library runs the existing six contexts plus dense Movie/Series metadata. Original Dispatcharr and dependency authors retain all application credit.

Use the existing recording-connect-cohort/prepare.py with --workflow catalogue-seven. The 31 Python payloads preserve the old six leaf bodies and use published, credential-safe Source evidence. The dense projection verifies all eleven sequences, full rows, typed causal clocks, independent UUIDs, stable owned credentials, exact bootstrap credential digests, provider/task/HTTP/ordered WS/catalogue behavior, and the actual request-origin cache URL. No private raw Source capture is required and no Source business workflow is replayed.

Public seven-context execution is pending. Private Candidate48 seven-context execution passed. Functional results do not accept performance or the full migration. Historical failures remain separate. Frozen byte-exact helper trailing whitespace is preserved for provenance.

From a clean checkout whose Rust inputs match the image, prepare and run using
the existing launcher (replace paths and Docker socket with local values):

```sh
git archive --format=tar --output=/tmp/dispatcharr-original.tar 1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430
python3 -B scripts/rust-integration/recording-connect-cohort/prepare.py prepare \
  --repo "$PWD" --workflow catalogue-seven \
  --build /absolute/path/to/candidate48/build.json \
  --source-archive /tmp/dispatcharr-original.tar \
  --docker-host unix:///absolute/path/to/docker.sock \
  --output /tmp/dispatcharr-public-seven
python3 -B scripts/rust-integration/recording-connect-cohort/prepare.py run \
  --plan /tmp/dispatcharr-public-seven/plan.json
```

The build record must come from a successful image build, not an edited manifest.
The launcher checks all Rust hashes, performs its UID/readability check, binds the
published Source evidence, and runs all seven contexts with bounded cleanup.
