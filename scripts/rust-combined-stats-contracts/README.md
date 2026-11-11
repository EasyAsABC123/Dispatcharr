# Combined stats — selected handoff

Original Dispatcharr authors retain all application/algorithm credit. Derived
DRF/Django template and CSRF attribution remains in the selected files and pinned
assets. No live Rust/Main/Cargo, Git index or milestone ledger was edited here.

`runtime-selection.json` selects17 exact files from the746-input current-source
union: Root's736 accepted inputs plus10 new files, preserving every unselected
Root hash. The existing VOD registry includes both normal refresh registration
and the new tiny CSRF helper; do not replace it with the older image47 registry.
`tool-selection.json` selects the two independently tested controller closures
under distinct `scripts/rust-combined-stats-contracts/{http,physical}/frozen-tools`
namespaces, so no existing VOD controller is overwritten. Native binaries and
private response/cookie data are excluded from the proposed repository selection.

The API `/proxy/stats/` now uses real admin/UI authentication and sequential
live, VOD and catch-up statistics, then samples the outer clock. Missing Redis
returns the Source error; projectors reuse existing Source-derived behavior and
fallbacks. Success is Django JsonResponse, including under HTML Accept; errors
and OPTIONS preserve the separately captured DRF formats. HEAD is405. The
slashless path is the Source SPA/CSRF behavior, not another JSON API or a redirect.
Actual integer-zero projector differences were fixed in their precise branches;
computed floating-point values remain floating point.

Accepted requests own supervised tasks and physical child guards. Losing the
HTTP observer does not release SQL/Redis ownership. Final response bodies retain
the same admission. Two requests reserve176MiB+128KiB within192MiB; bounded input
is64KiB and default serialized output24MiB. A5s observer deadline does not claim
physical completion. Shutdown closes admission immediately and joins Main's8s
service-drain stage. These reservations and existing row/field/transfer limits
are not universal allocator/RSS or TCP-flush guarantees. Native cap rejection is
an explicit resource extension, not a Source HTTP error-parity claim.

The successful HTTP reference covers60 actual Original/native normal-Main pairs:
authentication, methods, Accept/format/indent precedence, finite HTML/CSRF
contexts, slashless methods, and nonempty live1/VOD2/catch-up1 data. JSON types
and array order are strict. Only independently checked per-family clocks/VOD
durations/CSRF masks and the two actually observed Allow strings are qualified
variants; raw hashes/headers remain. Nineteen selected models and sequences are
unchanged. This does not exhaust all metadata orderings, malformed inputs,
session-auth/HTML contexts or every database/Redis key.

Four real physical controls cover blocked PG plus explicit observer-future
abort, a3.2s Redis pause plus abort, two held real projection bodies, and input/
output caps. Owners retain92,340,224B singly/184,680,448B for two frames until
actual settlement or body release, and drain to zero. The earlier52/55 native
negative and initial Python helper-name failure remain preserved.

After promotion, these commands reproduce the frozen controllers; use a fresh
output each time. The frontend donor build manifest must already exist, and its
image is only a static/frontend donor, not the native runtime under test:

```sh
source artifacts/rust/tooling/environment.sh
CARGO_TARGET_DIR="$PWD/artifacts/rust/target-combined-stats-replay" \
  cargo build --manifest-path rust/Cargo.toml --locked --offline \
  -p dispatcharr-rs --bin dispatcharr-rs
python3 scripts/rust-combined-stats-contracts/http/frozen-tools/scripts/rust-vod-api-contracts/run_provider_refresh_source.py \
  --repo "$PWD" --output "$PWD/artifacts/rust/combined-stats-http-replay-1" \
  --build-manifest "$BUILD_MANIFEST" --observer combined_stats_source.py \
  --native-stats-binary "$PWD/artifacts/rust/target-combined-stats-replay/debug/dispatcharr-rs"
```

For physical controls, compile the `dispatcharr-rs` test executable with
`cargo test -p dispatcharr-rs --bin dispatcharr-rs --no-run --message-format=json`;
take its exact `compiler-artifact.executable`, then pass it to the physical frozen
runner with `--observer combined_stats_physical.py --native-stats-binary <test-ELF>`.
Its ignored peer is environment guarded and never registered in production Main.
Offline comparison controls require no Docker/Django:

```sh
python3 -m unittest discover \
  -s scripts/rust-combined-stats-contracts/http/frozen-tools/scripts/rust-vod-api-contracts \
  -p test_combined_stats_compare.py -v
```

Root must still review/promote the selected bytes, build the canonical package,
replay unchanged React's Stats flow with the UUID action, and run appropriate
packaged regressions. No M2/full-statistics/browser/performance completion or
canonical image claim follows from these private component/normal-Main gates.
