# UserAgent compatibility and bounded mutations

The original Dispatcharr authors retain full application credit and the original AGPL license. This Rust slice follows the pinned original `core` UserAgent model, serializer, viewset and default UserAgent cache signals. It does not complete the backend migration or establish a performance gain.

## Implemented behavior

`/api/core/useragents/` retains Standard-level reads and administrator writes, active-account authentication and the UI network policy. POST, PUT, PATCH and DELETE use the original field names, uniqueness messages, required/blank/null handling, trimming and maximum lengths. JSON, URL-encoded forms and multipart requests are covered. Missing HTML boolean fields on full writes become false; blank HTML boolean values and omitted partial-update fields preserve their distinct original behavior. Read-only IDs/timestamps are ignored on input. Lookup failures precede body validation on existing-object routes.

Successful saves, including empty PATCH and changes to a nondefault agent, invalidate `coresettings:default_user_agent` and replace the persistent `coresettings:default_user_agent:ver` key. Failed validation does neither. Deletion clears StreamProfile and M3UAccount foreign keys, while leaving the configured default-ID JSON untouched. The default reader then uses the original fallback. Thirteen original ID conversion vectors cover floats, Unicode digits, underscores, whitespace, booleans, null, invalid collections and very large values. Inactive configured agents remain eligible, as in the original.

The existing version-fenced reader prevents an old in-flight SQL result from filling the cache after a newer write. Native cache strings are consumed by the actual original Python HTTP image consumer and CoreSettings getter in the disposable fixture.

Four mutation tasks can own work at once. Admission happens before body parsing; excess work returns 503. Once admitted and parsed, an owned task finishes its SQL/cache operation if its HTTP caller disconnects. Actual PostgreSQL lock tests verify four blocked inserts, a fifth rejection, abandoned callers, all four commits after unlocking and subsequent recovery. This explicit bounded overload behavior is a native operating limit, not a claim that the original imposes the same limit.

## Evidence

- [Source/native component summary](../rust-integration/verified-user-agent-source-native-3.json): 79 actual original HTTP cases, 13 default-ID vectors and native cache-fence/admission/cancellation checks using disposable PostgreSQL and Redis.
- [Immutable image20 summary](../rust-integration/verified-user-agent-packaged-image20-3.json): all 95 checks passed: the same 79 contracts, 13 ID vectors and three packaged resource/race checks.
- [Combined image20 regression](../rust-integration/verified-integration-m5-auth-image20-1.json): API1024, IP33, password/hash34, actual persistent React/logo16, logo101 cases/104 responses and the UserAgent scope. All owned Compose projects were removed.

Reports bind the exact application image, source tree, original request bytes, captured probe tools and raw report hashes. Only current-clock `created_at`/`updated_at` values are normalized; fixed original dates and database invariants remain exact. Content-Length is ignored only for JSON containing those clock values. JSON field order is canonicalized; request bytes are compared by SHA256.

Two packaged probe attempts remain recorded as failures. The first lacked a Python base64 import. The second repeatedly observed PostgreSQL statistics within one transaction without refreshing its statistics snapshot. The successful retry uses `pg_stat_clear_snapshot()` before each observation. Both fixes affect probe tooling only; the immutable backend image stayed unchanged.

Run the packaged gate with:

```sh
python3 scripts/rust-integration/run_user_agent_contracts.py \
  --output artifacts/rust/unique-user-agent-run \
  --candidate-build-manifest artifacts/rust/candidate-build-m5-useragent-m7-scoped-ip-image20-1/build.json
```

The runner requires a disposable original-image fixture and captures its tools before execution. It rejects an existing output directory, verifies candidate image/source labels, and removes its own project afterward. Raw ignored fixture artifacts contain temporary credentials and must not be published.

## Remaining coverage

Concurrent unique/delete-update ORM races, Redis authentication/ACL/fatal-error response shapes, skip-cache environment read behavior and malformed NUL/surrogate/file-valued fields need separate original/native gates. Whole-request and field limits outside the captured families are not asserted equivalent. No job execution, system-event production, complete browser administration workflow for UserAgents, or performance reduction is inferred from this slice.
