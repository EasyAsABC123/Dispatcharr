# Original React startup and authentication helpers

Full application credit remains with the original Dispatcharr authors and
contributors. Source contracts come from `frontend/src/App.jsx`,
`frontend/src/store/auth.jsx`, `frontend/src/store/settings.jsx`,
`apps/accounts/api_views.py`, `dispatcharr/utils.py` and `core/api_views.py`.

This slice was written during an isolated performance quiet window. After the
window, 38 store unit tests, 18 probe/helper tests, 12 runner tests, strict Clippy
and workspace compilation passed. The immutable candidate then passed
[419 focused HTTP checks and 38 isolated proxy checks](../rust-integration/verified-integration-m5-startup-relations-1.json).
The same immutable image subsequently passed the complete 655-check default
suite; the isolated proxy suite contributes 20 additional specific contracts.
The earlier combined 518-check report does not cover these additions.

The unchanged React startup checks `initialize-superuser`, tries refresh or
proxy sign-in, loads `users/me`, then settings/environment/version and initial
channel/profile/provider/EPG data. Failed status checks already fall back to
showing the ordinary login form; they do not permanently hold its spinner.
Provider/EPG stores catch their own fetch errors, so successful initialization
alone cannot establish complete settings/channel workflow compatibility.
The initial channel-ID request and TV Guide summary now have dedicated
`GET /api/channels/channels/ids/` and `/summary/` handlers. Both reuse level,
adult, profile, visibility, search/filter and effective ordering rules without
pagination or full stream graph materialization. Summary returns original
effective name/number/logo/EPG/group values. Their role/filter/profile/override
source comparisons passed within the focused channel read/write probes.

| Endpoint | Implemented contract | Remaining gate |
| --- | --- | --- |
| `/api/accounts/initialize-superuser/` | Any method returns `superuser_exists:true` once any account level >=10 exists, including inactive accounts; no credential/body/UI-network checks. With no admin, reads expose source IP/setup gate; disallowed POST has exact source 403. | Allowed first-install POST remains 501 pending bootstrap/migration verification. |
| `POST /api/accounts/auth/proxy-login/` | DRF credential validation, global UI network policy, enabled/header config, explicitly trusted immediate peer, exact/unique case-insensitive username then unique email lookup, active-account check, shared login budget, last-login update and JWT pair. Unavailable probes do not spend login attempts. Trusted success/ambiguity/inactive/level-zero/header/policy/JWT cases passed against Python. | Login audit/inbox events and distributed login throttling. |
| `GET /api/core/version/` | AllowAny with presented credential validation; pinned original 0.33.0 and null timestamp; optional `DISPATCHARR_VERSION` / `DISPATCHARR_VERSION_TIMESTAMP` release metadata. Role/invalid-token/global-UI-policy cases passed. | Immutable release metadata wiring. |
| `GET /api/core/settings/env/` | Active authenticated user of any level plus original UI network policy; source null location/TLS/environment shape when IP lookup is disabled. | Enabled IP lookup cache/background refresh remains 501, not fabricated pending/success. |
| Existing login/token/refresh/logout | Source CharField validation/coercion, alias preauthentication, refresh blocked-network error precedence, logout permission errors and JWT preservation passed source goldens. | Audit/session/CSRF and distributed throttle behavior remain pending. |

`startup_probes.py` runs as part of the existing `core` suite, so no integration
runner registry change is needed. Root's disposable seed disables optional
external IP lookup using the original CoreSettings model/signals, and both
backends see that same row. Reports should identify this fixture choice.
Bootstrap tests never remove/demote administrators. They assert that configured
POSTs leave user rows unchanged, even when sent a valid creation payload.
Generated login tokens are normalized only after independent HS256 signature,
token type, subject, unique JTI and bounded lifetime checks using the fixture
key. No key/token/password/body is recorded. Existing core/profile probes
continue comparing exact canonical JSON digests.
The alias token exchange also preserves the source's untrimmed post-login
username lookup: a padded valid username authenticates but leaves last_login
unchanged. Non-object login payloads preserve the source 500 status, with a
safe JSON diagnostic rather than its unported HTML exception body.
Single-IP network entries now have the source /32-/128 interpretation rather
than being silently rejected by a CIDR-only parser. Global-policy malformed
entry/strict host-bit errors and uncommon netmask syntax still need dedicated
source vectors; this addition is not a claim of complete network parser parity.

The queued run sequence after the quiet window is store formatting/unit/Clippy,
Python probe/runner unit tests, an immutable candidate build, focused
`--contract-suites streams,core` comparisons and then the complete suite.
The earlier 145 focused stream / 543 combined counts describe the 25 relation
additions alone; startup adds further checks and the actual report must supply
the definitive totals. Missing source behavior must remain recorded as failure
or an explicit unsupported gate.
The complete suite waits a recorded 62 seconds between account CRUD password
checks and startup sign-in checks. They share the real 3/minute per-IP login
budget, and foundation spends an additional Rust attempt. Application throttle
state is neither reset nor bypassed. The initial full attempt retained its two
budget-interaction failures; the subsequent 655-check run passed on the same
image with that separation. Focused endpoint proofs were not reclassified.

The separate enabled-proxy oracle is opt-in and excluded from `all`:

```sh
python scripts/rust-integration/run.py --output artifacts/rust/proxy-auth-1 \
  --candidate-image UNIQUE_IMMUTABLE_TAG --candidate-build-manifest BUILD_JSON \
  --contract-suites proxy_auth --trusted-proxy-fixture
```

That flag adds a hashed overlay explicitly trusting peers on both disposable
fixture services; their published ports remain loopback-only. The suite changes
source fixture reverse-proxy/network settings, creates only `rust-probe-*`
users, and restores/removes its exact rows in `finally`. Dedicated synthetic
client IPs isolate the 3/minute login budget. Cases cover exact/case-insensitive
identity, unique/ambiguous email/username, inactive and level-zero users, identity
whitespace, disabled proxy, invalid presented credentials, per-user versus
global UI policy, refresh validation precedence and configured setup/version
policy exemptions. All 20 proxy-specific comparisons plus 18 shared
schema/auth/media checks passed on the same source-bound candidate image.
