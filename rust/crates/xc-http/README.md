# XC account and live output migration

Original Dispatcharr authors and contributors retain full application credit.
The account/live component is in the workspace and attached to the application
when Redis is configured. Packaged application verification is pending. The
host comparison in `artifacts/rust/m7-xc-native-6` passed 76 actual original/native
HTTP cases plus seven real PostgreSQL/Redis resource tests. This does not complete
the XC/XMLTV migration or establish a performance improvement.

The initial leaf implements the original two-pass integer channel assignment.
A compressed successor map preserves its first-free choices while
avoiding repeated linear scans for many null/fractional numbers. Arbitrary
integer decimal strings retain finite-float values outside i64 range. Both
392 extracted original assignment cases pass. Its conservative allocation
reservation still needs adversarial high-row/allocation checks; no performance
result is implied.

The draft now also includes canonical player/panel routing, account/live DTO
projection, row-streaming PostgreSQL readers, and a cancellation-safe Redis
connection counter. It reuses the output crate's existing trusted-origin and
CSRF checks through additive wrappers. Captured missing-cookie and valid-header
CSRF behavior matches. Unsafe multipart CSRF, unusual regex path aliases, and
uncaptured error renderers remain open.

The handler admits work before auth/SQL/Redis awaits, reserves shared build
memory, and keeps work owned after a caller drops its request. Final bytes hold
their allocation-capacity reservation through the final cloned body frame.
Channel/category rows are read incrementally under row/metadata limits. The
live response is serialized entry by entry into a capped response allocation,
using draft ASCII/spacing rules rather than constructing a second whole list.
The panel writes and drops categories before loading live rows. Numbering has
a separate conservative reservation obtained without waiting while another
build reservation is held. Real paused-Redis cancellation, SQL-lock cancellation,
and retained-frame/shutdown tests passed. Missing-group creation matches actual
Source defaults under eight concurrent native callers. General allocation/large-row stress
and packaged shutdown remain separate gates.

Counter scans use bounded Lua reply pages over the existing shared Redis
transport. They preflight selected field sizes before returning metadata, hold
reply and request admission until the real response completes, and distinguish
explicit capacity rejection from the original helper's zero-on-error behavior.
The Source constructor uses the shared ingestion numeric parser. A pinned
Python3.13.18 / Unicode15.1 corpus passed 2,827 conversion cases including exact
finite bits and signs; 67 actual original Redis-count cases also passed against
the native counter. Capacity rejection remains distinct from a successful zero.

Recognized movie/series/EPG actions remain explicit 501 drafts rather than
falling through to account information. Their Source on-demand refresh and
XMLTV cache/dummy-programme behaviors are separate open gates. Canonical
`/player_api.php` and `/panel_api.php` routes are attached, while detailed-info
refresh, movie/series/EPG actions, XMLTV, and full milestone acceptance remain
open. The live long/short entry adapter is now main-wired to the existing media
dispatcher; its 36 captured Source auth/method/lookup wire cases pass in the host
component. Positive generated-URL playback and packaged static/shutdown behavior
remain unverified. Catalog success does not imply playback acceptance. Resource
measurements require a later immutable, equivalent-side-effect paired run.
