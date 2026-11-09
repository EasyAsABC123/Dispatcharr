# Native M3U output slice

All original application credit belongs to Dispatcharr authors/contributors.
Source: `apps/output/views.py` and its channel/provider helpers. Retained AGPL
license and attribution apply. Original Django6.0.9 CSRF compatibility fixtures
and adapted token handling retain Django authorship and BSD license in
`data/Django-LICENSE`. No M7 completion or performance gain is claimed.

The native host router matches50 original HTTP cases on actual Django
PostgreSQL/Redis, including anonymous versus XC network/auth order, inactive
XC users, output-specific channel/profile ACLs, overrides/logos/radio, exact
M3U/query/URL bytes, HEAD, method and valid/invalid CSRF behavior. Source/native
each record33 download and4 blocked system events; original Django reads the
native cached export string successfully. A separate actual-SQL component test
matches30 positive body hashes and rejects two capacity overflows. Strict
all-target Clippy passes. See `scripts/rust-output-contracts/verified-m3u-host.json`.

The reader streams bounded SQL rows with effective overrides and explicit
metadata/channel caps. Original and native also match a6,000-channel,
3,492,158-byte export cold/warm/after expiry, with exact hashes and Python↔Rust
large cache-string interoperability. Export size defaults8MiB (hard64MiB),
metadata8MiB (hard32MiB), with an independent shared192MiB memory budget
(hard512MiB). Both build and retained body/frame allocations are admitted;
Redis operation guards retain admission across caller cancellation until actual
replies drain. A real delayed-Redis proxy test covers this cancellation edge.
Large unpolled response tests cover independent shared-budget rejection, drop
recovery and shutdown. The general Store/auth cache codec keeps its1MiB limit;
output uses an isolated bounded string-only pickle subset.
See `scripts/rust-output-contracts/verified-m3u-large-host.json` and its README
for configurable limits, conservative charging formula and reproduction flags.
This is not a process-RSS or aggregate Redis-server memory claim. Exports beyond
configured/hard bounds fail explicitly without truncation. Nonempty direct
profile/credential transforms remain unported.

The gateway router/shutdown is app-wired and strictClippy-clean. Real fixture
tests verify unpolled response admission, drop recovery and shutdown without
client polling. Two pending-input tests cover stop before/after body-read
subscription. Two-second cache staleness/expiry and event counts match Source.
Profile names with raw ampersands, literal plus, Unicode and percent literals
also match after replacing a query-form path decoder with one-pass URI decoding.
Frozen application Compose comparison remains an acceptance gate. Broader CSRF/token/origin/parser
corners, exports beyond supported bounds, Connect/plugin audit dispatch and resource testing remain
open. XMLTV, full Xtream and HDHomeRun follow; unchanged React frontend behavior
must be verified after actual application wiring.
