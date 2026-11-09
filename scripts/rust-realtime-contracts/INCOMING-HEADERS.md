# Incoming HTTP header compatibility

The original Dispatcharr authors and contributors retain full application credit.
These probes execute the pinned original HTTP application against disposable
original-schema PostgreSQL/Redis, then the native application. No request
middleware or permission helper is replaced with a mock.

The original production WSGI boundary decodes header values as Latin-1. A value
containing invalid UTF-8 must remain available to `get_client_ip`: an invalid
forwarding hop is skipped while the remaining public hop still determines the
ACL. Dropping the entire header incorrectly substitutes the trusted private
socket peer. For example, raw X-Forwarded-For bytes `203.0.113.42, ff` (where ff
is one byte, not those two letters) must select 203.0.113.42 and deny a private
network allowlist. The unchanged original does so.

The same boundary combines repeated fields in order with comma-space, and
hyphen/underscore names share a CGI META key. A first private XFF field must
not hide a later public field. Combining two proxy identity assertions must not
silently authenticate the first account. User-Agent bytes also retain their
original Latin-1 meaning: e9 becomes é, while UTF-8 bytes c3a9 become Ã©.

`contracts::source_headers` now provides that name/value normalization for all
11 adapters that construct PlaybackRequest. Owned HeaderMap conversions use
iteration over every field rather than dropping duplicate continuation entries.
The three origin readers also consume the normalized values. Existing raw
Authorization/API-key guards still reject ambiguous or unsupported credentials;
Bearer versus API-key selection order stays unchanged. Protocol-specific
upstream response parsing and CSRF/cookie parsing are separate boundaries.

Evidence is retained chronologically:

- `m7-incoming-headers-before-1`: probe import collision before HTTP comparison;
  cleanup completed.
- `before-2`: real image18 ACL/User-Agent witness. Initial proxy/origin cases were
  ineffective and existing auth error wording was also compared; do not use this
  run as proof of proxy/origin handling.
- `before-3`: corrected immutable image18 witness, 36 actual cases and 24 observed
  differences: 20 ACL decisions, proxy login, forwarded origin and two UA events.
- `fixed-1`: frozen whole-app host build matched those 36 cases, then a separate
  duplicate-field audit exposed remaining failures.
- `duplicates-1`: 51 cases, 10 differences from first-value XFF lookup.
- `duplicates-2`: 58 cases, 17 differences including underscore aliases, combined
  proxy identities and exact repeated-UA metadata.
- `fixed-2`: frozen whole-app host build matched all 58 cases and exited zero;
  isolated Compose cleanup passed.

The 58 cases cover real HDHR, users/me, TS status, HLS session management and
catchup stats permission boundaries, plus proxy-login POST, forwarded origin,
M3U blocked-event metadata and malformed presented authentication. Comparisons
use status and selected non-secret response fields, token-pair presence, and
actual database event details. The two malformed-auth cases assert rejection 401
only: existing fail-closed guard wording is intentionally not a payload-parity
claim. Tokens remain in memory and never enter command arguments or evidence.

The witness requires a trusted proxy peer and private-only policies, both
configured explicitly in the owned fixture. Invalid-only forwarding still falls
back to the private peer, matching the original; valid public hops still deny.
No new trust policy, public endpoint, performance result, or complete-app
acceptance follows from this fix. Frozen image19 subsequently passed all 58 cases in
`m7-incoming-headers-image19-2`, bound by
`verified-incoming-headers-image19-1.json`. The first packaged attempt remains:
`m7-incoming-headers-image19-1` matched 57 cases, then correctly returned 429
because the two backends shared the same three-attempt login budget. The rerun
records deletion of only the disposable fixture login history (two prior
entries) between independent Source/native phases. Separate shared-throttle
proofs are unchanged.


## Scoped and IPv4-mapped IPv6

A subsequent Source audit found another address-parser boundary. Python accepts
scoped IPv6 such as `2001:db8::1%header-scope`. CIDRs compare its numeric address;
audit metadata and authentication history keys retain the scope text. For
IPv4-mapped IPv6 (`::ffff:203.0.113.42%header-scope`), the original helper returns
IPv4 text and drops the scope. Empty or repeated percent scopes are invalid and
follow the original fallback behavior.

`m7-scoped-ip-before-1` confirms 27 differences on unchanged image19: 25 ACL
decisions plus two audit metadata cases. `m7-scoped-ip-fixed-1` passes the same
95 cases on a frozen whole-app host build. `fixed-2` adds two actual shared
Redis API-key history deltas, proving scoped and mapped addresses use the same
keys as the original; all 97 pass. `verified-scoped-ip-host-1.json` binds it.

The shared SourceIpAddr representation keeps numeric IpAddr values for CIDRs
and original normalized text for metadata. PlaybackViewer retains that text;
VOD captures it before its existing typed-peer normalization, and Catchup media
uses an additive text resolver method. Audit records and shared authentication
keys use the preserved text. Existing caller header/request limits bound scope
strings. Other uncharacterized IP grammar remains audit backlog. The scoped helper
normalizes IPv4-mapped socket peers for trust checks as well as forwarded values;
CIDR consumers retain their numeric address API.

Frozen image20 passes all 97 cases in `m7-scoped-ip-image20-1`; see
`verified-scoped-ip-image20-1.json`. Its image ID is
`sha256:fd449c861ba84d44229004c961c92b263ac06c7fe21e6dc194b59a3a70cb4dac`.
The same explicit fixture login-history isolation is recorded, and Compose
cleanup passed. These 58/97-case families do not imply all forwarded-address
syntax, complete M7, full application, or resource-performance acceptance.
