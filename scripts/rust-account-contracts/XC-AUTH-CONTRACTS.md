# Shared XC authorization and channel visibility

This is a compatibility port of original Dispatcharr behavior. All original
application credit belongs to the original authors and contributors; see
ATTRIBUTION.md. The shared helper passed actual original-source PostgreSQL/Redis comparison:
21 auth cases, 7 channel cases, cooldown/lockout/envelope and retention assertions.
The sanitized `verified-xc-store-auth-1.json` binds captured helper/workspace/report
hashes. Frozen application HTTP/media and native M7 transport gates remain pending.

`AuthService::authorize_xc` takes a PlaybackRequest, username, password and endpoint
network key and returns User or StoreError. The sequence is system network,
existing failed-auth budget, missing credentials, exact username lookup, system
and user network checks, then constant-time UTF-8 comparison of the custom
`xc_password`. There is no Django-password fallback, `enable_xc` restriction or
active-user check. The source intentionally accepts an inactive user with valid
XC credentials; native access-JWT authentication remains active-only. Legacy
non-string stored passwords follow original Python str semantics, including
Unicode15.1 collection representation.

Network settings honor the shared Django group cache and version guard. Global
allowlists retain strict source parsing and early-match behavior; user lists
strip entries, ignore invalid CIDRs and accept host bits as the original does.
Malformed trusted settings remain server errors rather than password denials.
Network denials and successful credentials never consume the failure budget.

The shared key is `:1:throttle_xc_auth_<normalized-client-IP>`, with original
primitive-pickle newest-first float timestamps, 10 failures per 60 seconds. Failure
number 10 still returns 401, emits the lockout event and attempts an inbox notice;
request 11, including a correct password, returns 429. Retry-After is the source
integer floor, clamped to 1; an overfull legacy history can omit it. The typed XC
denial returns the source `error` JSON without introducing a Bearer challenge.
Rust failure writers use bounded CAS retries on the original cache bytes;
legacy Django writers remain their original non-atomic read/modify/write. No
mixed-deployment atomicity claim is made.

Lockout storage reproduces the `login_failed` detail keys, no channel, and the
original retention algorithm. Its cutoff row remains, so max 1 retains 2 rows;
the delete predicate is ID-based even though the cutoff selection orders by
timestamp. Logging/inbox failures never turn a rejected credential into success.
Configured Connect/plugin event dispatch is a separately tracked M8 bridge.

Inbox notice creation claims the original shared 3600s cooldown with cache.add
semantics, collector-deletes the old notice and dismissals, then creates the
admin-only high-priority warning. Deletion commits before creation, preserving
the source failure boundary; a failed creation clears the cooldown. A configured
UpdateEventSink receives the exact source update/system_notification envelope.
Native ChannelsRedis delivery belongs to the M7 adapter; a missing sink is
reported and remains an explicit delivery gap.

`Store::channel_for_xc_user_by_id` preserves source level/adult/profile membership
visibility, admin bypass and absence of a hidden-output filter, with no active
check. Missing and invisible channels both return None. Caller routing must run
XC authorization before parsing channel IDs, and must return StoreError directly
to preserve Retry-After. Native UUID/session auth continues using its separate
active-user rules.

Verified helper oracle tooling:

- `scripts/rust-integration/xc_fixture.py`: disposable original-Django helper,
  21 auth cases, 7 channel cases, failed-only lockout, cooldown replacement,
  dismissal cleanup, actual original Channels message and retention cutoff.
  It emits no passwords or tokens.
- `store/tests/xc_sql.rs`: explicitly ignored until the original fixture and
  PG/Redis URLs are supplied; asserts the disposable flag and compares real
  SQL/Redis outcomes. Its captured EventSink checks the exact envelope but
  establishes no native transport or resource claim.

Unusual malformed-body HTML responses, hostile non-source cache objects, Redis
TLS variants and configured Connect/plugin integration execution are outside
this slice's exact-response claim. No first-install schema migration is added.
