# Candidate54: credentials, auth routes and plugin metadata

Original Dispatcharr, Django, DRF and dependency authors retain application,
protocol and algorithm credit. React is unchanged. Commit
`7148f05a68f73345de5103249695bf12382600e8` contains eighteen Rust paths copied
exactly from SDK86 into an isolated sparse checkout. The primary checkout's
five protected VOD files and index remain unchanged.

[SDK86](../scripts/rust-integration/milestone-batch54/SDK/index.json) passes all
four gates: formatting, workspace check, strict Clippy and workspace tests.
There are 1,193 passed tests, zero failures and 786 ignored tests across 180
result blocks. All 1,450 Rust inputs match the image source. The earlier SDK84
sandbox attempt passed its first three gates, then four localhost-listener
tests failed with EPERM; identical-source SDK85 passed with the required local
socket access. SDK86 additionally includes auth compatibility and the corrected
VOD SQL test. Ignored tests are not counted as behavioral acceptance.

The [targeted PostgreSQL resolver gate](../scripts/rust-integration/milestone-batch54/resolver-PG/index.json)
passes using the existing Compose bootstrap, supervisor, seed and filtered
`actual_django_movie_episode_series_xc_profiles_flags_and_caps` test. It checks
Movie/Episode credential transformations and pool identity while retaining a
genuinely unsupported-transform control. The 60.052-second execution reuses the
sole Cargo target; no Source HTTP battery is replayed. One exact container,
its captured original host generation and the project are independently absent.

The Docker build passes in 318.151 seconds as image
`sha256:1c8bbbab484eaf32e64261fdd139584cd97a044e92565ea9c476de0e654cde43`,
with source-tree SHA256
`77aca44e7dfc7d66ed919a0446bf73ab36755ff1f61e2104b24b948b42377d75`.
[All six package gates](../scripts/rust-integration/milestone-batch54/package/index.json)
pass in 31.938 seconds, with 55 exact package containers independently absent.
Store and Main recompile, and Main's ELF differs from Candidate53.

The implementation adds transformed VOD credentials and shared pool identity,
Source auth method/metadata responses and observed cookie logout effects,
plugin application response headers, outbound connection headers, and startup
developer-notification synchronization through existing owned transports.
Full Django admin login/session creation is not implemented by these changes.

The fifteen-context Native application batch is running with the corrected
Comskip terminal fence and the calibrated complete own-input CUT content check.
The separate twelve-case metadata phase exits zero in 130.619 seconds, but four
report-related bound inputs changed after launch. Its successful observations
and six-container/five-generation cleanup remain recorded; application
acceptance is false pending a frozen rerun. Backend Date/Connection profiles
are separately validated, with literal header identity and peer TCP FIN
unclaimed. Source authority and the earlier failed captures remain unchanged.

These facts establish no new application CPU or memory gain and close no
complete M0–M9 milestone. Native integration, remaining compatibility domains,
and a fresh quiet paired performance campaign are still required.
