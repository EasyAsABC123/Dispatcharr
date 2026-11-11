# Packaged missing/stale provider modal

Original Dispatcharr and dependency authors retain application/algorithm/license
credit. This gate uses normal packaged Main and unchanged React; no standalone
peer, fake provider response in the frontend, task replacement or eager mode.

Source dispatch is synchronous: Movie api_views.py229 calls the Task object
(refresh_movie_advanced_data, tasks.py3310); Series api_views.py582 calls the plain
refresh_series_episodes helper (tasks.py1422). The separate batch Series task is
not the modal path. No worker/queue qualification is inferred.

A private prepared SQL template captures all18 tracked model rows and sequences,
including PKs, existing UUIDs, timestamp inputs, initial null/stale25h refresh
states and the normal-login password hash. Each backend independently prepares,
executes the browser, captures its effects and restores baseline. Native starts
from the exact Source prepared template; it cannot merely read Source-fresh
metadata. A Source-only browser/restore/reseed/restore controller probe passed
before candidate execution. Redis login/rate/cache effects remain genuine and
owned; no global Redis reset or whole-DB rollback is claimed.

The actual image48 Main gate passes normal login, default-priority versus
UI-first-provider distinction, both Movie/Series provider selections, refreshed
visible details, provider-specific replacement episodes and12 real2x3 image
checks across both backends. All four relations refresh in each own browser
interval. Both sides make the same ordered XC GET raw URIs and header values;
header names are case-insensitive. Artwork request transport is retained but is
not folded into that XC comparison. Images may reuse legitimate shared cache.

All18 tracked SQL row sets compare in predeclared PK order, with strict recursive
JSON types (true is not1), keys, arrays and field values. Only30 identified changed
SQL datetime paths validated in each own browser interval and four new Episode
UUIDv4 paths may differ. Pre-existing UUIDs/input clocks remain exact. No wholesale
body/timestamp/order normalization occurs. The exact ordered visible UI fields,
account selections, episode names and proxied-image paths/query/dimensions compare;
complete HTTP body byte identity and unseen body clock/UUID serialization are not
new claims of this browser gate. Their separate component reports retain their
original execution/source scope.

Retained history: Source1 had a test-only Series.plot assumption (Source emits
description), Source2 passed; Source3 validated exact reset/reseed. Packaged48-1
passed the original consumer. Before publication the SQL consumer was hardened
against Python bool/int equality; nine offline controls and a fresh unchanged
image48 paired run48-2 passed with the stronger recursive type guard. Runtime,
scenario, input data and budgets were unchanged; all old reports remain intact.

Reproduce with Docker/Compose, pinned Original Git/image, Node with WebSocket,
Chromium and a passing immutable candidate build manifest:

```sh
python3 -m unittest discover -s scripts/rust-vod-api-contracts -p test_stale_modal_controls.py -v
python3 scripts/rust-vod-api-contracts/run_provider_stale_modal_browser.py \
  --source-only --build-manifest <candidate-build.json> --output <new-Source-check>
python3 scripts/rust-vod-api-contracts/run_provider_stale_modal_browser.py \
  --build-manifest <same-candidate-build.json> --output <new-paired-check>
```

The driver inspects actual image/source labels and extracts the unchanged frontend
from that manifest. It accepts no host-peer binary. The source archive is checked
against full pinned f2323ac7…fa7ff. Raw provider wire, SQL snapshots, browser reports
and synthetic credentials stay private; publish only selected sanitized fields,
counts and hashes. All tracked rows/sequences restore and all owned containers,
networks and volumes must disappear. Login/Redis effects are removed with the
owned project. Existing cached-only44 reports/tools remain unchanged. This adds
functional image48 stale-modal acceptance, not M6 queue/default graph, complete
WebSocket-effect parity, resource gains or full milestone acceptance.
