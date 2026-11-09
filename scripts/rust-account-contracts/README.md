# Account mutation compatibility probes

`probes.py` is imported by the real PostgreSQL/Django/Rust Compose integration
runner. It never runs against arbitrary accounts: URLs must be loopback HTTP,
and both supplied users must have the disposable `rust-probe-*` prefix.

Interface:

```python
probes.run(http, baseline_url, candidate_url, accounts, record)
probes.run_crud(http, baseline_url, candidate_url, accounts, record)
probes.run_app_data(http, baseline_url, candidate_url, accounts, record)
```

`accounts` is the isolated seed output (`admin` and `viewer` each have `id`,
`username`, and `access`). `http(url, body=None, token=None, method=None,
headers=None)` returns `(HTTP status, JSON body)`. `record(name, passed, details)`
receives only static names, status values, booleans, exception class names, and
canonical JSON SHA-256 digests; API keys, tokens, passwords, and serialized user
fields are never included. The caller must retain the underlying fixture and
propagate recorded failures as a nonzero integration exit status.

Tests cover owner PATCH protection of admin fields/properties, null-key deletion,
legacy-null preservation when a first-name-only PATCH omits properties,
custom-property merge/null validation, navigation/email/name errors, own key
generation/revocation, cross-backend API-key authentication, malformed target
IDs, and denial of explicit target users to non-admins. Random key values are
normalized only in documented key-generation response comparisons; status and
all other response fields remain exact. Key shape and cross-authentication are
checked independently.

The two backends share a disposable database. Before each owner PATCH, the
reference admin API restores the same known initial fields/properties so a
baseline write cannot mask a candidate no-op. Owner test fields are restored
afterward. An original SQL/JSON-null properties field is tested before any explicit
property writes; subsequent property API writes normalize it to an object just as
the original serializer does, and its logical empty contents are restored.
API-key lifecycle tests intentionally rotate/revoke only the fixture
viewer's key; admin keys are checked to remain unchanged under denied requests.
There are no production/provider requests or bulk account mutations.

Run tool tests with:

```sh
python3 -m unittest discover -s scripts/rust-account-contracts -v
```

These tests verify the probe harness. Account parity requires executing the
module against both real backends in the Compose integration runner.

`run_crud` adds admin User/Group create/read/PATCH/PUT/delete lifecycles,
duplicate names, required/invalid foreign-key fields, non-admin mutation denial,
M2M replacement, and user deletion with a real dependent app-data row. Each
backend creates the same temporary unique name only after the prior lifecycle
has deleted it. Only the generated top-level object ID is normalized. Explicit
date fixtures preserve exact timestamp comparisons; relation IDs, custom JSON,
roles and other response fields remain untouched. Password changes created by
Rust are checked through both real login endpoints; tokens and passwords are
never recorded.

The source channel-profile API creates one disposable `start_empty` relation
fixture shared by both implementations. Cleanup verifies each object's current
name before deleting it, refuses protected seeded account IDs, and removes that
profile after the lifecycle suite. Missing, unavailable, or failed fixture setup
records a failed check instead of skipping it as a pass. These probes never
target existing non-fixture user/group names.

`run_app_data` checks owner/admin aliases, cross-user denial, namespace and JSON
validation, compact Unicode/float/big-integer byte accounting, numeric unchanged-
value semantics, 128 KiB values, the 1 MiB aggregate quota, and the 25-app quota.
Reference and candidate complete independent lifecycles with fixture cleanup
between them so one implementation cannot mask another's missing write. A
26-request contended insertion phase checks exactly 25 successes, one quota
rejection, and the resulting count/size. Its scheduling-dependent winning keys
are not compared; status counts and quota totals remain strict.

Only the disposable viewer's app-data rows are cleared by that suite. Existing
non-fixture users are never targeted. Missing services and cleanup failures are
recorded as failed checks rather than skipped. These golden checks remain
functional evidence; they do not establish account/API performance acceptance.
