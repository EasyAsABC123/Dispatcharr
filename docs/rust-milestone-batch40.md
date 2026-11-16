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
Actual clean-commit verifier execution remains pending. The scoped Source mapping
checks pass; the broader existing register module retains a separately proven
pre-existing notification digest failure, whose evidence/assertion was not edited.

## Complete affected batch

At this source checkpoint image/package and application execution are pending.
One controller will run aggregate refresh four phases, real station HTTP four
phases and VOD ingestion three cases in dependency order, with retained
Source55/53/58 and no reference business replay. Reuse the existing Compose
fixture/bootstrap with explicit state and Main-generation boundaries; namespace
and cleanup failures block further dispatch. Individual case success cannot
promote the whole batch. Preserve the earlier failed aggregate and raw promotion
media attempts. Required broader roles, hot cache/update, metadata/Series, plugin,
concurrency/loss, soak/rollback and paired resource gates remain open. No new
application CPU or memory gain is claimed by compilation or test counts.
