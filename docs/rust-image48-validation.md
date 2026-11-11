# Image48 validation and reproduction

All Dispatcharr application and algorithm credit belongs to the original authors and contributors. React and LICENSE are byte-identical to upstream revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

This checkpoint builds committed application inputs from `fda49b16d5e3aca1e2c95ce01fc2b4d558e77572`: 746 Rust inputs and 497 frontend/license/attribution inputs. The measured image identity, source archive, compiler checks and each independently cleaned fixture are recorded in the checkpoint proof. Functional acceptance does not transfer image47 performance numbers or complete a milestone.

## Freeze the tools and build

Run from a clone containing the migration branch and upstream history, with Docker Compose available. Choose fresh empty destinations; preserve failed runs.

```sh
set -euo pipefail
repo_root=$(git rev-parse --show-toplevel)
validation_root=$(mktemp -d /tmp/dispatcharr-image48.XXXXXXXX)
git -C "$repo_root" archive fda49b16d5e3aca1e2c95ce01fc2b4d558e77572 scripts docker | tar -x -C "$validation_root"
export GIT_DIR="$repo_root/.git"
export GIT_WORK_TREE="$validation_root"
cd "$validation_root"
python3 scripts/rust-integration/build.py \
  --git-revision fda49b16d5e3aca1e2c95ce01fc2b4d558e77572 \
  --output "$validation_root/build" --tag dispatcharr-rust-candidate:image48-replay --timeout 1800
manifest="$validation_root/build/build.json"
image=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["image_id"])' "$manifest")
```

The explicit Git context matters: launching a frozen tool directory inside another checkout without it can produce an empty upstream archive. Three initial setup failures are preserved in the baseline proof. The tools and application source are read from committed inputs; pending workspace edits are excluded.

## Packaged regression commands

Use distinct fresh `--output` directories for every command. Each runner binds the immutable image manifest and attempts cleanup only for its owned Compose fixture; inspect the actual cleanup fields and retained failures. The batch includes the following gates:

| Gate | Script under `scripts/` | Additional arguments |
| --- | --- | --- |
| 18 baseline suites | `rust-integration/run.py` | `--candidate-image "$image" --candidate-build-manifest "$manifest" --contract-suites all` |
| Proxy authentication | `rust-integration/run.py` | `--candidate-image "$image" --candidate-build-manifest "$manifest" --contract-suites proxy_auth --trusted-proxy-fixture` |
| Password hashes | `rust-integration/run.py` | `--candidate-image "$image" --candidate-build-manifest "$manifest" --contract-suites auth_hash --trusted-proxy-fixture` |
| User agents | `rust-integration/run_user_agent_contracts.py` | `--candidate-build-manifest "$manifest"` |
| Output profiles | `rust-integration/run_output_profile_contracts.py` | `--candidate-build-manifest "$manifest"` |
| Profile restart | `rust-integration/run_output_profile_contracts.py` | `--candidate-build-manifest "$manifest" --redis-restart` |
| Guide grid | `rust-epg-contracts/control_reads.py` | `--candidate-image "$image" --build-manifest "$manifest" --candidate-grid-wire` |
| Current programmes | `rust-epg-contracts/control_reads.py` | `--candidate-image "$image" --build-manifest "$manifest" --candidate-current` |
| VOD/timeshift | `rust-m4-perf/run.py` | `--candidate-build-manifest "$manifest"` |
| Clean installation | `rust-integration/run_clean_install.py` | `--candidate-build-manifest "$manifest"` |
| Category React | `rust-vod-api-contracts/run.py` | `--mode http --packaged-categories --category-closed-gaps --category-browser --build-manifest "$manifest"` |
| Catalogue HTML/React | `rust-vod-api-contracts/run.py` | `--mode http --packaged-unified --packaged-html --unified-browser --build-manifest "$manifest"` |
| Static/Chrome HTML/React | `rust-vod-api-contracts/run.py` | `--mode http --packaged-unified --packaged-html --packaged-static --unified-browser --build-manifest "$manifest"` |
| Provider actions | `rust-vod-api-contracts/run_provider_actions_packaged.py` | `--mode http --packaged-provider-actions --build-manifest "$manifest"` |
| Cached provider metadata | `rust-vod-api-contracts/run_packaged_cached_info.py` | `--mode http --packaged-cached-info --build-manifest "$manifest"` |
| Channel artwork | `rust-integration/run_logo_contracts.py` | `--candidate-build-manifest "$manifest"` |
| VOD artwork | `rust-vod-api-contracts/run_packaged_vod_images.py` | `--mode http --packaged-vod-images --build-manifest "$manifest"` |
| Provider modal | `rust-vod-api-contracts/run_provider_modal_browser.py` | `--build-manifest "$manifest"` |
| Live React/Stats | `rust-live-browser/run.py` | `--build-manifest "$manifest"` |

The category command intentionally requires zero exclusions and eighteen additional exception responses. Omitting `--category-closed-gaps` selects an older four-exclusion mode; that incorrect initial invocation failed despite 163 exact responses and passing concurrent reads. The failed report remains preserved.

The complete import workflow is Original-first and then native, using a fresh real guide clock:

```sh
python3 scripts/rust-job-contracts/run_refresh_workflow.py --output "$validation_root/import-original"
python3 scripts/rust-job-contracts/run_refresh_workflow.py --output "$validation_root/import-native" \
  --manifest "$manifest" --reference "$validation_root/import-original"
```

Run those consecutively. The original guide's declared window ends two hours after capture. Replaying an expired historical reference caused a real current-programme assertion failure; that attempt remains recorded. Do not expand the window or normalize away current-programme semantics.

The live, category, catalogue, clean-install and provider-modal browser gates require local Chromium and Node22 or newer with the capabilities checked by their runners. They exercise normal React login and real HTTP, not patched UI components. Synthetic credentials remain fixture-owned.

## Separate gates and limits

Retention, two-worker lifecycle, safe operator CLI and stale-provider modal evidence are separate from this regression batch. The lifecycle aggregate remains failed for Original's provider-slot leak after owner death, while native cases pass. Recovery and the default scheduler/worker graph remain OFF. CPU/memory measurements require coordinated host quiet and their own fixed protocol; these functional commands do not sample or establish performance gains.
