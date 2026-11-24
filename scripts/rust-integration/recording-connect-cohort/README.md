# Portable recording Connect acceptance cohort

All application, model, task, protocol and algorithm credit belongs to the original Dispatcharr authors and the third-party projects identified in the repository [attribution](../../../ATTRIBUTION.md) and [license](../../../LICENSE). These tools preserve their implementation and exercise the Rust migration; they do not claim authorship of Dispatcharr.

The default **Source1→Native1 recording Connect workflow passed runtime qualification** on Candidate37 from a clean checkout: both backends exited 0 and exact cleanup passed. [The qualification packet](../recording-connect-cohort-qualification/qualification.json) binds 113 unchanged executable/data payloads; this README status/build guidance is the only later documentation change. Captured historical payload values are replaced with public shapes and fresh, locally collected Source authority. This is not a byte-exact reproduction of the private helpers and does not close full M0, migration parity or performance gates. The optional --all-native branch was not executed by this public qualification and retains the known open strict restart failure. [The historical batch37 evidence](../../../docs/rust-milestone-batch37.md) remains separate.

The existing Fixture, Workflow, real Source producers, Origin and canonical process coordinator are reused. No additional server is provided. The default run executes one fresh Source recording context, verifies stopped/completed events with four original successful deliveries and four caught capture HTTP 400 deliveries, then runs the corresponding Native context. Both use owned disposable Compose databases, Redis and media. Native observes its actual claimed UUID, stop HTTP 200/end, direct-delete refusal, cold/warm enrichment, templates, strict media decode and history cutoff. General plugin/recovery/deletion hook parity and independently observed per-event Host/SQL receipts remain open.

From the matching repository checkout, generate build.json with the [existing Docker builder](../README.md), then prepare and run the default cohort:

```sh
python3 scripts/rust-integration/build.py \
  --output /tmp/dispatcharr-connect-build \
  --tag dispatcharr-rust-candidate:connect-check
python3 -B scripts/rust-integration/recording-connect-cohort/prepare.py prepare \
  --repo "$PWD" --output /tmp/dispatcharr-connect-fresh \
  --build /tmp/dispatcharr-connect-build/build.json --docker docker
python3 -B scripts/rust-integration/recording-connect-cohort/prepare.py run \
  --plan /tmp/dispatcharr-connect-fresh/plan.json
```

Preparation is offline: it validates the Rust files against the image manifest, creates the pinned Original archive using Git, hashes public dependencies and writes a fresh plan. The pinned Original commit must exist locally. `--source-archive` accepts an already generated archive only when its exact hash matches. `--docker-host` supports a separate Docker daemon. The explicit `run` command first verifies actual UID 65532 read access in one read-only, networkless container and verifies its removal; it then starts the selected applications. The pinned Original image and candidate image must be available to that Docker daemon.

The launcher takes the same scripts/rust-milestone-batches/.dispatcher.lock exclusive lock as the canonical dispatcher and runs Source and Native serially. Interrupt requests finish the currently bounded cohort and its finally cleanup, then stop before the next backend. Forced termination cannot establish cleanup. If actual cleanup/pipe/group proof is incomplete, a repository-local cleanup-required marker prevents this portable launcher from starting another run until the exact owned resources have been checked. The marker is not an approval flow or evidence of resource absence.

The output directory is private. Full Source payloads, local URLs, task frames, database observations and media stay there. The Source run creates `locally-collected-contract.json`; Native compares every normalized field against that fresh authority. Only public keysets, protocol semantic values/types and a historical Source42 contract hash are shipped in `public-shapes.json`. No captured Source42 URLs or paths are included. Pure controls can verify a locally held historical or newly collected contract without printing its values:

```sh
python3 -B scripts/rust-integration/recording-connect-cohort/check-equivalence.py \
  --private-reference /private/path/to/contract.json \
  --output /tmp/private-shape-equivalence.json
```

`--all-native` adds the five independent M8/boundary/restore/grouped contexts and nonempty restart to the fresh Native run. Nonempty restart currently has a known strict decoder failure; it remains failed rather than becoming an accepted scenario. No historical private results bypass these fresh executions. A selected run does not establish full M8, all datastores, soak, rollback or resource gains.

The 107 cohort Python files and two adapters are exported with original credit headers. Historical captured JSON and assembly metadata are omitted. The existing 224 canonical baseline/bootstrap support files are reused from `scripts/rust-playback-baseline`, `scripts/rust-baseline`, the port/account helpers and `docker/compose.rust-playback.yml`. Legacy artifact-only validation CLIs now require explicit local input paths. They are not part of the default application run. The default path/authority changes passed the linked Source/Native qualification. Wider contexts, interruption/fault campaigns, independently observed per-event Host/SQL receipts and complete M0 coverage remain open.

### Redirect provider framing

Use `prepare.py prepare --workflow redirect-provider-framing` with the
final ordinary build manifest to run one `redirect-common` context per
application. The fixture holds a short, fixed-length provider GET after
HEAD refusal. It checks that the signed request remains pending while the
credential pool is held, then verifies fallback to the alternate provider
and exact SQL, Redis, and process retirement. The same owned role also
observes the original encoded query on the provider request. The
engine-free fixture controls passed. Final signed Compose verification
still needs the selected release image.

Original Dispatcharr and dependency authors retain full credit.
