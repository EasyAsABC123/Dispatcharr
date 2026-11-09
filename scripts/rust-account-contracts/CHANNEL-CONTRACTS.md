# Channel compatibility gates

Original application behavior and credit belong fully to Dispatcharr's original
authors and contributors. See [ATTRIBUTION.md](../../ATTRIBUTION.md). Sources:
`apps/channels/api_views.py`, `serializers.py`, `models.py`, `signals.py`,
`access.py`, `utils.py` and `compact_numbering.py` at the pinned Python baseline.

The immutable combined image recorded in
[verified-integration-m5-memberships-channel-reads-1.json](../rust-integration/verified-integration-m5-memberships-channel-reads-1.json)
passed 274 real Compose checks: the previous 190, 37 single/bulk membership checks,
and 47 channel read checks. This includes native MPEG-TS decoding; HLS/fMP4 use
separate protocol evidence bound to the same image. This establishes functional
contracts, not CPU or memory improvements or complete M5 acceptance.

Channel reads preserve raw and effective override fields, ordered stream IDs,
optional nested stream serialization, source provider information, level/adult/
assigned-profile restrictions, list visibility, common filters/order and bounded
SQL pagination. Only pagination next/previous test-server origins are normalized
in the oracle. Other field types, IDs, URLs and query parameters compare exactly.
Non-admin nested stream responses omit provider URLs as the source requires.
Uncommon quoted search parsing, unusual coercions, HTTPS/proxy-origin construction,
OPTIONS/method errors and non-finite numeric renderer failures remain edge gates.

Profile membership rules deliberately retain source differences:

- Profile CRUD reads require level 1; mutations require administrators.
- Single membership PATCH requires an active authenticated UI user, including
  level zero when assigned the profile. Exactly level 10 can access every profile;
  other levels use their assignments, matching the original ownership queryset.
- A missing membership is inserted disabled before single-update validation. A
  subsequent 400 leaves that row behind; the next bulk update reports updated=1.
- Bulk membership PATCH requires an administrator and validates all channel IDs
  before any mutation. Existing duplicate IDs count as multiple updates with the
  final value winning. Repeated new IDs trigger the source uniqueness failure;
  exact original HTML500 rendering remains incomplete.

The later [356-check immutable run](../rust-integration/verified-integration-m5-channel-crud-2.json)
passed 79 channel create/PATCH/PUT/delete and stream/override/profile relation
oracles, alongside all previous suites and additional coercion/search vectors.
The same image subsequently passed all 356 checks with captured test-tool bytes,
including while later repository probe edits differed from those captured files.
The [405-check group run](../rust-integration/verified-integration-m5-channel-groups-1.json)
also verified manual/provider group CRUD, claim/unclaim and provider rename with
demotion, then repeated all 405 checks against the shared-input replica image.
Its real TS cases verify inactive stop/delete, active stop/delete ending media,
and default deletion keeping an established stream alive. A focused 29-check run
adds 11 cleanup/FK gates on that exact replica image. Only the source's unordered
`deleted_groups` collection is sorted for comparison: repeated Django lifecycles
changed its raw order. Names/multiplicity, counts, messages, unrelated group
identity, stream SET_NULL and override-masked raw-group SET_NULL remain strict.
Important source rules:

- Channel numbers are nullable floats and duplicates are permitted. Omission on
  creation chooses the first available positive integer, reserving both raw and
  override numbers. Omitted groups resolve to the manually owned Default Group.
  A default-assigned effective number remains an integer in the CREATE response,
  while its FloatField representation is a float; a later GET reads both as
  floats. The first 344-check run found this distinction and retained its failure
  hashes before the corrected 356-check run passed.
- Omitted/null profile selection or numeric zero adds all profiles; an empty list
  adds none. Explicit IDs add only those profiles. Invalid selection returns 400
  **while committing the already-created channel**, because the source returns a
  response inside its transaction rather than raising an exception.
- Scalar/override/stream changes share a transaction. Streams replace and order
  the relation. Catchup capability rolls up from active provider accounts.
  Source mutation responses may still contain the pre-rollup scalar values;
  the following GET exposes the new database values. The oracle checks both.
- Overrides apply to auto-created channels. Omission preserves, null deletes,
  an empty object preserves, and individual nulls clear fields. An all-null row
  is removed. The source checks the old auto_created value on an update.
- Unknown, read-only ID and UUID fields are ignored. Hidden channels remain
  reachable by their ID for editing and deletion.

The Rust handlers currently fail with explicit 501 before unsupported effects:
new channel EPG assignment (cache invalidation/programme import), changed override
EPG assignment, compact-numbering auto-channel scalar updates, deleting channels
with recordings (DVR task revocation/schedule cleanup), and stop_stream=true
without the engine control callback. The optional ChannelControl trait and
AuthService builder invoke configured teardown outside the delete transaction,
then repeat the lookup before deletion as the source does. Teardown is
best-effort: a configured hook error/inactive stream does not prevent deletion.
The historical 356-check image has no application hook configured. The later
405-check image has the configured hook and passes the real TS stop/delete
contracts; other-format/follower teardown uses separate distributed media gates.
The remaining EPG, compact numbering and recording side effects are incomplete compatibility gates,
not passing CRUD cases. Core settings/profile/user-agent writes still need their
separate job/cache/notification work in [CORE-CONTRACTS.md](CORE-CONTRACTS.md).

All mutation probes create only rust-probe-* resources in disposable databases,
verify names before cleanup, and retain only statuses and hashes. Generated root
channel IDs and UUIDs are the only channel mutation fields normalized. Relation
IDs and fixture timestamps remain exact; read-only UUID preservation is checked
before normalization. Missing services or failed cleanup fail the suite.
