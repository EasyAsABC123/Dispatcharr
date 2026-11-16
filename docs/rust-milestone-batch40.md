# Candidate40: linked M6 production workflows

Original Dispatcharr and dependency authors retain application, algorithm,
protocol and library credit. React and licensing remain unchanged. This joins
three affected workflows into one integration candidate; full M6 and overall
migration acceptance remain open.

The aggregate refresh fix distinguishes exact authenticated broker observations
from legacy scanner scopes. Only the private broker scope, minted after actual
head equality and SHA binding, avoids a scanner-specific null-parent-result
requirement. Raw frame/account/root/parent checks, SQL/file admission and exact
CAS stay intact. Legacy scanner scopes still require their parent receipt.
This addresses [Native56's actual failure](rust-milestone-batch39.md), where the
aggregate parent succeeded but its local children never claimed.

Native Schedules Direct HTTP producers now use the existing SD worker capability.
The supported create action inserts the source, disabled schedule and links with
Source-compatible change-marker semantics, commits and physically closes SQL,
then publishes real countdown-3 station work. Native lineup addition reuses SD
auth/cache/HTTP and publishes immediate station work after SQL closes. API auth
uses the Source 15-second path while existing worker defaults remain unchanged.
Other source types, schedules, CRUD and error domains retain explicit guards.
Creation preserves Source null updated_at and SQL-null custom-properties behavior.
The [Source53 reference](../scripts/rust-integration/channel-stations-source53/README.md)
will be compared through real Native HTTP, without a Source-publisher fallback.

The fresh Movie VOD slice adds the natural refresh action and actual default-
queue root on the existing shared XC slots, Auth pool and provider session.
It creates real basic Movie, provider-relation and category rows and emits the
Source updates/backend result. Inline helpers are not invented as new workers.
The supported domain is a fresh basic nonempty Movie catalogue with empty Series,
without metadata/language splitting or automatic M3U-to-VOD handoff.
[Source58](rust-vod-ingestion-source58.md) supplies actual HTTP controls,
provider/SQL/cache/event and catalogue-read authority. Repeat/update/Series and
other producer paths remain separate work.

## Validation before packaging

[SDK56](../scripts/rust-integration/milestone-batch40/checks.json) passes all four
gates with unchanged inputs: formatting, workspace check, strict all-target
Clippy and workspace tests. There are 1,141 passed tests, zero failures and
784 ignored fixture tests across 180 result blocks; 1,438 inputs include 1,432
Rust inputs. Ignored tests are not application or physical acceptance.
SDK54 already passed the runtime code. SDK56 binds the final two-file Docker
verifier closure; no Rust compilation was needed for those packaging changes.
A manifest-field mismatch stopped an earlier overlay application before changing
files; the intermediate unchanged-input SDK55 pass remains in local artifacts.

The verifier's Dockerfile-specific ignore/COPY closure now includes exactly the
23 public Source51/53/55 authority files required by contract tests, while private
artifacts remain excluded. Building a verifier image does not run its CMD tests.
The [clean-commit Docker verifier](../scripts/rust-integration/milestone-batch40/verifier/index.json)
builds successfully and both focused groups exit zero: 23 Source-contract tests
pass, and the evidence-register group passes its public reproduction/control test
with one optional private-raw test skipped. Both owned containers are independently
absent. The full verifier CMD was not executed. The scoped Source mapping
checks pass; the broader existing register module retains a separately proven
pre-existing notification digest failure, whose evidence/assertion was not edited.

## Complete affected batch

The committed candidate `a119544a2fd2e19ab3934cd40cc9e5c66968ff6b` built
successfully in 142.248 seconds. Image
`sha256:ea39fd7ad7dee75b71a6c8df89fd219d2e240f9d4d2be1abefcf717828b48f38`
passes all [six package-family probes](../scripts/rust-integration/milestone-batch40/package/inventory.json),
with actual exit zero for every probe. Independent cleanup confirms all 55
owned container IDs are absent.

The [combined application batch](../scripts/rust-integration/milestone-batch40/application/qualification.json)
now passes all 11 required cases in one actual controller, one reused Compose
bootstrap and three Native contexts, with actual exit zero in 164.960 seconds.
Standard output/error reached EOF and the process group settled. Database,
cache and media reset between contexts, and each used a fresh Main generation.
Source55, Source53 and Source58 remain the retained reference authorities;
no Source business workflow was replayed. Native59/60/61 are projections of
this single run, rather than three independent executions.

| Context | Cases passed | Qualified behavior |
| --- | ---: | --- |
| Aggregate refresh | 4 | Actual HTTP roots, active-account membership, ordered child claims/results, default custom-account error behavior |
| Station discovery | 4 | Real Native source-create/lineup HTTP, countdown admission without early effects, station SQL/provider/progress and partial provider failures |
| Basic VOD ingestion | 3 | Non-XC/disabled rejection without publication; natural HTTP 202 and queue execution, fresh Movies/relations/categories and empty Series |

Station producers use actual Native HTTP with no Source-publisher fallback.
The VOD success case creates two Movies, two provider relations and three
category relations, makes five actual TLS provider requests and emits two
updates; its catalogue read succeeds. These are the bounded fresh-catalogue
cases, with no metadata/update/nonempty-Series acceptance implied.

[Cleanup](../scripts/rust-integration/milestone-batch40/application/cleanup.json)
confirms all five fixture container IDs and their recorded positive original
host PIDs are absent, with empty project container/network/volume inventories.
The separate UID-probe container is absent; its host process generation was
not independently captured. A fresh independent read-only cleanup check also
passes. The [execution record](../scripts/rust-integration/milestone-batch40/application/execution.json)
binds the exact raw controller, aggregate report and candidate binding by SHA.

The earlier failed aggregate and raw promotion-media attempts remain unchanged.
Required broader roles, hot cache/update, metadata/Series, plugin,
concurrency/loss, soak/rollback and paired resource gates remain open. Full M6
and the overall rewrite remain unaccepted. No new application CPU or memory
gain is claimed by this functional batch.
