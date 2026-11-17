# Candidate54 remaining implementation batches

Original Dispatcharr authors and contributors retain all application, model,
protocol and algorithm credit. This audit plans compatibility work; it grants
no milestone, application or performance acceptance.

The inspected baseline is commit `7148f05a68f73345de5103249695bf12382600e8`,
Candidate54 image
`sha256:1c8bbbab484eaf32e64261fdd139584cd97a044e92565ea9c476de0e654cde43`,
and [SDK86's exact inputs](../scripts/rust-integration/milestone-batch54/SDK/inputs.json).
The immutable SDK86 source, route composition and selected Source workflows
were read. This is a confirmed backlog, **not an exhaustive count of all gaps**.
Source coverage, conditional features and runtime acceptance need separate review.

## Current follow-up

Candidate55 implements the two profile detail methods. Candidate57 implements
the other nine baseline bindings for the characterized bounded positive domains;
the unconditional mutation stub is removed. This does not establish general CRUD
parity: XC/multipart/new-URL account shapes, nested groups and VOD-related account
deletion, broader EPG credential/source-type changes, linked channel collectors
and SQL fault behavior remain open. The [Candidate57 acceptance record](rust-milestone-batch57.md)
retains a real generated-task integration failure after successful account creation.
The historical table below identifies the original implementation batches, rather
than eleven currently unconditional refusals. The other implementation domains
and milestone-wide criteria below remain open.

## Immediate implementation sequence

| Batch | Confirmed missing implementation at Candidate54 | Milestones |
| --- | --- | --- |
| Candidate55, in progress | Nested account-profile detail PUT/PATCH, two method bindings. Validate the implementation before building; it is not part of accepted Candidate54. | M4, M5 |
| Provider account lifecycle | Account collection POST and detail PUT/PATCH/DELETE: four bindings. | M5, M6 |
| Profile lifecycle | Nested profile collection POST and detail DELETE: two bindings. | M5, M6 |
| Guide-source lifecycle | EPG-source detail PUT/PATCH/DELETE: three bindings. | M5, M6 |

All eleven baseline bindings dispatch to the authenticated unconditional
[`provider_reads::mutation` stub](https://github.com/EasyAsABC123/Dispatcharr/blob/7148f05a68f73345de5103249695bf12382600e8/rust/crates/store/src/provider_reads.rs#L440).
After the two Candidate55 methods land, nine remain. Server-group and scoped
filter CRUD use implemented handlers and are excluded from this count.

The credential-six workflow needs four profile PUTs and one core-settings PUT.
The latter is already implemented: [CoreSettings detail methods](https://github.com/EasyAsABC123/Dispatcharr/blob/7148f05a68f73345de5103249695bf12382600e8/rust/crates/store/src/core_settings_writes.rs#L146)
are composed with special effects in Main. Do not implement another settings
handler merely because the library's default guarded constructor can return501.

## Reuse and required Source characterization

**Profile create/delete first.** Reuse the reviewed PUT/PATCH field validation,
parent scoping, account authorization and existing writer admission. Reuse
[`xc_profile_notifications`](../rust/crates/job-transport/src/xc_profile_notifications.rs)
and the existing notification publisher/terminal ownership for expiration effects.
Capture creation defaults, required patterns, expiration/JSON synchronization,
default-profile deletion, cascades, notification dismissals and removal of deleted
IDs from users' `allowed_m3u_profile_ids`. Inspect the actual routed ViewSet:
the presence of `Serializer.destroy` alone does not prove Django invokes it.
[Source serializers](../apps/m3u/serializers.py#L33) and
[profile deletion signals](../apps/m3u/signals.py#L103) define those questions.

**Account lifecycle next.** Capture default-profile creation and max-stream sync,
typed custom-property preferences/hash overrides, expiration forwarding, locked
accounts, activation status, cron preservation and interval scheduling. Creation
can publish group refresh and `playlist_created`; deletion must stop attributed
auto-created channels outside the database transaction, then perform transactional
cascades and task cleanup. See [Source serializer creation](../apps/m3u/serializers.py#L392),
[signals](../apps/m3u/signals.py#L12), [default-profile model callback](../apps/m3u/models.py#L393)
and [account deletion](../apps/m3u/api_views.py#L355).
Reuse provider mutation validation/admission, existing M3U/XC task actors,
channel-stop ports and event delivery. `scanner_files::schedule`,
`database_schedule`, `calendar_schedule` and `normal_beat` provide existing
schedule machinery; they do not automatically reproduce model-save ordering.

**EPG detail writes next.** Capture dummy-source EPGData creation/name updates
and WebSocket effects, dummy timer disabling, cron/interval preservation,
activation status, deleted refresh tasks and protected cached-file cleanup.
[Source EPG signals](../apps/epg/signals.py#L13) include recursive save boundaries.
Reuse [SD source write validation/SQL](../rust/crates/store/src/sd_source_writes.rs),
existing scheduled-refresh/index actors, publisher and owned file operations.
Its finite zero-interval SD create path is not authority for uploaded XMLTV,
positive intervals, cron or general source updates.

Useful retained authority, with limits:

- [Provider write contracts](../scripts/rust-account-contracts/PROVIDER-WRITE-CONTRACTS.md)
  qualify server groups/filters, not the nine missing methods.
- [Credential Source1](../scripts/rust-integration/m4-credential-pool-source1/index.json)
  retains four profile-save effects and five redirect cells; its controller exit1
  remains literal. [Source2](../scripts/rust-integration/m4-credential-pool-source2/index.json)
  retains the settings transition and real shared/distinct/unlimited readers;
  its original controller also exits1. Neither is general CRUD authority.
- [Auth/session Source reference](../scripts/rust-integration/auth-route-session-source1/index.json)
  supplies retained authentication behavior, not new model-signal semantics.
- [Notification producer inventory](../scripts/rust-account-contracts/NOTIFICATION-PRODUCERS.md)
  describes Source envelopes; its historical Native status labels need checking
  against current code. Prepared admin/alias cohorts are not executed authority.

For each lifecycle batch, capture the missing finite Source transitions first.
Retain full affected rows/sequences, task publications, cache, files and events;
then implement against those observations. Run SDK/build/package gates and one
complete affected Native acceptance cohort. Do not infer lifecycle parity from
serializer tests or replay already-qualified Source scopes unnecessarily.

## Other confirmed implementation domains

| Domain | Actual missing behavior or refusal branch | Milestones |
| --- | --- | --- |
| Channel/guide coupling | Two branches reject EPG-backed channel creation and changed EPG override: [channel writes](../rust/crates/store/src/channel_writes.rs#L372). Ordinary channel CRUD is separate. | M5, M6 |
| Schedules Direct lineup | The selected handler explicitly refuses non-POST methods, naming GET/DELETE/OPTIONS: [SD HTTP](../rust/crates/store/src/sd_source_http.rs#L92). Match the Source method contracts before implementing. | M6 |
| M3U direct output | Nonempty provider-profile allowlist transforms are refused: [reader](../rust/crates/output-http/src/reader.rs#L69). | M7 |
| Guide HTTP variants | Browsable guide HTML, current-programme multipart and byte-offset fallback have explicit unimplemented branches: [programme HTTP](../rust/crates/store/src/epg_program_http.rs#L392), [current HTTP](../rust/crates/store/src/epg_current_http.rs#L128), [current lookup](../rust/crates/store/src/epg_current.rs#L322). Ordinary JSON is separate. | M6, M7 |
| VOD recovery | Two refusal domains remain: adoption when the requested ID differs from the retained session ID, and adoption across provider profiles: [SDK86 sessions](https://github.com/EasyAsABC123/Dispatcharr/blob/7148f05a68f73345de5103249695bf12382600e8/rust/crates/vod-http/src/sessions.rs#L480). Same-profile public-idle transitions already have Source255/265 implementation; Source238 identity transitions are also present. Protected workspace WIP removes accepted paths and must not be integrated wholesale. | M4 |
| Timeshift recovery | Five explicit busy/idle/existing/legacy-cross-worker/programme-profile adoption refusals in [media pool](../rust/crates/timeshift-http/src/media_pool.rs#L366), plus remote fingerprint adoption in [media](../rust/crates/timeshift-http/src/media.rs#L466). Counted as missing transitions, not six missing endpoints. | M4 |
| Django admin | Main has no admin router; `/admin/*` reaches the [backend404 catchall](https://github.com/EasyAsABC123/Dispatcharr/blob/7148f05a68f73345de5103249695bf12382600e8/rust/crates/app/src/main.rs#L1098). The prepared27-model Source cohort must establish actual workflow coverage. | M8 |
| SQLite | `DB_ENGINE=sqlite` explicitly aborts startup: [database configuration](../rust/crates/app/src/database_config.rs#L18). PostgreSQL success does not satisfy the SQLite criterion. | M5, M8 |

## Conditional domains and untested requirements

Backup create/status/restore501 branches depend on a missing job adapter;
Main attaches it through selected jobs. Plugin repositories also have actual
fetch/signature execution paths. Their older unsupported messages or comments
are not evidence that those complete features are absent. Default activation,
plugin installs/actions/signatures and broader compatibility still need tests.
Likewise, the HLS legacy metadata-profile refusal differs from its typed-profile
manager/output-store path; establish Source reachability before assigning work.

Capacity limits, unsupported value/parser shapes, unknown-action fallbacks and
uncompiled prototype comments were not counted as unconditional endpoint stubs.
They still require contract-level review. All formal M0–M9 remain open: complete
React/API roles, default worker behavior, failures/restarts, supported databases,
full-service performance, two-hour soak and rollback rehearsal remain required
by the [milestone criteria](rust-milestones.md).
