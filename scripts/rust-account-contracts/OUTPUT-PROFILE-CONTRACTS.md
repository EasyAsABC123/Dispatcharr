# Core OutputProfile writes and remaining StreamProfile gates

Original Dispatcharr authors and contributors retain full application credit and AGPL licensing. The authoritative contracts are the pinned original `core/models.py`, `core/serializers.py`, `core/api_views.py`, `core/signals.py` and `core/tests/test_output_profile_scrub.py`. This slice implements management APIs; it does not claim active profile transcoding, job execution or full backend parity.

## Implemented API and original behavior

The unchanged React API calls POST/PUT/DELETE on `/api/core/outputprofiles/` and its detail route. Native writes now merge with the existing AuthService list/detail router. Active administrators at level10 or above can write; UI network policy and authentication precede object lookup and body validation. Existing Standard-level reads retain their separate permissions.

Fields are `id`, `name`, `command`, `parameters`, `is_active`, `locked`. ID is read-only. Name is required, unique and max255; command is required, nonblank and max255; parameters is required and nonblank with no model maximum. JSON, forms and multipart are covered. JSON model defaults, missing/blank HTML booleans, partial updates, readonly/unknown fields, malformed JSON/MIME and null/nonobject bodies follow captured original responses. A whole JSON-null body returns `non_field_errors: ["No data provided"]`; it is distinct from a null field and other nonobject body errors.

Creation may create a locked profile. Existing locked profiles reject changed fields in model order: name, command, parameters, locked, is_active. Serializer validation runs first, so invalid/duplicate values can take precedence. Equal-value updates and empty PATCH can succeed; a locked profile cannot be unlocked. Locked deletion retains the original list-shaped400 error. OutputProfile has no permitted user-agent exception; StreamProfile does.

## Ordered deletion, cache and rollback semantics

Deletion removes matching `custom_properties.output_profile` keys from user object maps, preserving unrelated fields. PostgreSQL JSON equality selects integer, canonical string and integral numeric matches; padded strings, unrelated references, SQLNULL and nonobject roots retain their distinct behavior. User IDs are paged128 at a time and unrelated property JSON stays inside PostgreSQL rather than being retained in Rust.

Next, the original **shared cached** stream-settings group is read. Its `hdhr_output_profile_id` is converted using Python int semantics and cleared only if it matches the deleted ID. The DVR group's `output_profile_id` follows. Existing group names and other map keys are retained. Nonobject DB values become an empty map on update. A stale cached match can cause an update despite a different live DB map; a cached match with a missing DB row can create the group.

Saving stream settings invalidates its group/value version and the resolved default UserAgent/value version. DVR invalidates only its own group/version. Versions are persistent. Missing-group get-or-create emits an initial save/invalidation and a second save after merging. Existing-row lookup does not consume its sequence; actual row IDs and both sequence states are compared in the fixture. These direct model signals do not invent DVR rescheduling, rehash jobs or output-worker stop events.

The original post-delete callback catches cleanup exceptions. Three actual Redis WRONGTYPE cases preserve its partial effects. Four actual PostgreSQL trigger faults establish the less obvious boundary:

- Failure updating a user, existing stream settings or existing DVR settings returns204 while the surrounding delete transaction rolls back. Earlier Redis invalidation may remain despite SQL rollback.
- Failed missing-group creation rolls back an inner get-or-create savepoint, allowing earlier deletion/user cleanup to commit. Subsequent cleanup stops.

Native behavior matches these outcomes rather than converting every cleanup problem into an atomic all-or-nothing operation or an invented HTTP error.

## Bounded ownership and shutdown

Four admitted owned mutation tasks survive abandoned HTTP callers; a fifth rejects503. Request parsing uses the existing8MiB service limit. This is an explicit native operating bound, not a claim the original imposes identical overload limits.

Redis uses one owned driver, at most16 physically pending commands, one-MiB value reads and two-second observer deadlines. Each pending command retains both its physical permit and its writer's Arc lease until the actual reply/disconnect. Canceling an observer or reaching its deadline does not release admission early. There is no unbounded waiting queue. Group reads retain the original cached-map/default/version-fence algorithm and distinguish connectivity fallback from server errors.

Shutdown closes writer admission, waits for ownership, then closes/joins the actual Redis driver when necessary and waits for retained leases to drain. It reports incomplete remaining SQL/body work rather than claiming cleanup. The app includes this service in its existing coordinated shutdown join. Real delayed-reply tests exercise cancellation, an actual command deadline, overload, reply recovery and forced driver shutdown; real PostgreSQL locks exercise abandoned owned inserts and recovery.

## Evidence and commands

[The frozen component summary](../rust-integration/verified-output-profile-source-native-9.json) binds127 actual original HTTP/SQL/cache cases, three Redis server faults, four SQL faults and both resource groups to exact source/tool/executable/report hashes. It compares the native write router merged with the full AuthService router. All owned Compose projects were removed. Strict app+Store all-target Clippy, two pure units and two probe-helper namespace tests passed separately. No new performance gain is asserted. Six cache-miss/default/empty/nonobject-map cases cover full copied DVR defaults and cached-empty authority. Nine actual Source vectors use BIGINT rows at2^53,2^53+1,+2 and BIGINT_MAX: float truncation is compared with exact integer IDs, never with `float(id)`, and huge user JSON references retain their types.

```sh
python3 scripts/rust-integration/run_output_profile_contracts.py \
  --output artifacts/rust/unique-output-profile-component-run
```

The runner freezes Rust before starting Compose so later sibling edits cannot change compiled bytes. It uses a locked offline graph and does not regenerate the shared lockfile. An explicit `--source-only` mode captures original behavior without claiming native execution. Packaged application replay uses `--candidate-build-manifest <successful canonical build.json>` and verifies the exact image binding. The packaged probe covers all captured contracts and three additional real SQL ownership/admission/recovery checks. Its actual immutable-image result remains pending until the parent builds the canonical candidate.

Only JSON key order and generated cache-version clock facts are normalized. IDs, sequences, null storage, numeric/string/bool types, fixed business values and request byte hashes remain exact. Raw ignored fixture artifacts contain disposable credentials and must not be published. Failed fixture/capture attempts remain recorded in the sanitized summary.

## Remaining gates

Packaged replay and actual React add/edit/delete for the profile UI remain separate gates. Cache race families beyond the captured scenarios, concurrent uniqueness/delete-update/user-property updates, Redis ACL/fatal SET families, oversized/malformed surrogate/NUL/file fields, OPTIONS/CORS and deployment-level SQL/body shutdown faults need additional original/native coverage. SQL JSON removal preserves concurrent unrelated keys more strongly than Django's loaded-object save; that concurrency family is explicitly unverified. Active transcoding and native jobs remain separate milestones.

StreamProfile JSON CRUD and four SET_NULL collector relationships already have earlier regression probes. It still needs source-compatible form/multipart boolean behavior for `is_active` and `locked`, whole-null and malformed JSON messages, Unicode/underscore/large IDs, fractional FK coercions and owned-work resource gates. Its older extractor/key helper must not be assumed equivalent to this OutputProfile slice. The common M5 whole-null-body family discovered here also needs explicit probes for other serializers before changing their accepted routes.
