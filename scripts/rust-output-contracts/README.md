# Native output migration

All original application credit belongs to the Dispatcharr authors and
contributors. The React UI is preserved; these are backend compatibility probes.
No M7 output parity or performance gain is established yet.

Start with M3U output, then XMLTV, Xtream metadata/playback aliases and HDHomeRun.
Original entry points are `apps/output/views.py`, `apps/output/urls.py` and
`dispatcharr/urls.py`; source tests include `apps/output/tests/test_views.py` and
`test_xc_auth_responses.py`. Native realtime transport is a separate workstream.

## Small testable steps

1. Capture actual original Docker HTTP responses for anonymous M3U and XC
   `get.php`, with an actual Django schema/Redis fixture. Cover method/auth
   order, profiles/ACL, overrides, query formatting, logos, catch-up tags,
   provider-direct output, content cache and system-event effects. Preserve
   failures. Unit tests patching middleware/network/logging are useful source
   references but do not replace actual HTTP evidence.
2. Add a native bounded renderer and SQL reader for the first captured cases.
   Verify exact UTF-8 body bytes and stable headers, then actual source/native
   PostgreSQL/Redis behavior. Unsupported provider transforms remain explicit
   until their own contracts are ported.
3. Wire the route and shutdown into a frozen Rust application image. Compare
   original/native HTTP again using copied, hashed tooling. Verify concurrent
   requests, cancellation/held bodies, bounded export/cache admission and error
   rollback before accepting this slice.
4. Expand source/native edge cases and shared-cache/event effects. Measure
   export resource/latency behavior with identical fixtures after functional
   equivalence. Do not reuse live-viewing performance ratios for exports.
5. Repeat these gates for XMLTV, Xtream and HDHomeRun; retain full-M7 and whole
   application gates until all required protocol variants are covered.

## Initial source audit

`/output/m3u`, `/output/m3u/`, `/output/m3u/<profile>` and the trailing-slash
profile alias are anonymous network-controlled surfaces. They ignore incoming
Bearer/API-key/query credentials. They first apply the global `M3U_EPG` network
policy, logging `m3u_blocked` and returning JSON403 on denial. Anonymous HEAD
returns an empty200 M3U attachment before validating profile existence. Other
requests reach the generator, which declares GET/POST/HEAD. Actual POST CSRF
behavior must be captured: the inner generator is exempt, but the selected
outer URL view is not decorated exempt.

`/get.php` first calls the shared XC authenticator with `XC_API`, including the
original inactive-user semantics, failed-only throttle and lockout effects.
Failures log a blocked download. It then invokes the same generator with that
user. It does not add the anonymous wrapper's `M3U_EPG` check or early HEAD
shortcut. User-facing playlist visibility caps channel levels even for admins;
admin outputs skip adult/assigned-profile filtering, while other users apply
level/adult/optional enabled assigned-profile membership. Anonymous profile
names select exact enabled memberships; a missing profile raises404.

All outputs exclude `hidden_from_output`, resolve effective overrides and order
by effective channel number (no invented tie-breaker). Names, numbers, radio,
group, logo, tvg-id and guide station attributes use effective fields. A null
channel number is blank in tvg-chno and uses channel PK for default tvg-id.
Whole float numbers format as integers; fractional values use Python display
formatting. M3U attribute/name strings are emitted literally by the original;
do not introduce new escaping rules as part of the migration.

The request origin follows `build_absolute_uri_with_port`: forwarded host,
port and scheme are accepted only from trusted socket peers, with original
standard-port handling. Plain EPG URLs use `/output/epg` and optional profile,
preserving last values of `tvg_id_source`, `cachedlogos`, `days`, `prev_days` in
original key order. Stream query parameters preserve `output_profile` followed
by `output_format`; `output_format` wins over legacy `output`. Query encoding
must follow Python `urlencode`, including literal `~` versus escaped `*`.

Plain streams use `/proxy/ts/stream/<uuid>`. XC streams use credential-bearing
`/live/<username>/<password>/<pk>` and `/xmltv.php` URLs; capture their unredacted
body hashes privately and persist only placeholder-safe diagnostics. Plain
proxy playlists do not advertise catch-up. XC proxy playlists can advertise
UTC/default catch-up and the timeshift query template; direct provider output
advertises XC catch-up only from the actual emitted stream's XC flags.
Catch-up booleans honor user/system flags.

`direct=true` is available anonymously and to XC admins; XC nonadmins continue
to use proxied URLs. Anonymous/unrestricted direct uses the first stream's
stored URL, falling back to the proxy. An explicit allowed-provider-profile list
selects the first eligible stream/profile with active account/profile and
credential transform; an empty/malformed allowlist omits unavailable channels.
Multicast UDP restores `udp://@` when the parsed IP is multicast.

Original content caching lasts2 seconds and keys profile/user/raw grouped query
plus origin; a cache hit skips rendering and download-event logging. The
client-specific download-event deduplication key separately uses an IP/UA MD5
prefix and2-second TTL. The response is `audio/x-mpegurl` with attachment filename
`channels.m3u`; it adds no export-specific cache header or ETag. Record common
Django security/CORS headers separately from endpoint headers.

## Next actual-source matrix

Use a disposable actual schema with visible/hidden/adult/high-level channels,
null/fractional/scrambled numbers, effective overrides (including false radio),
HTTP/local logos, groups, enabled/disabled memberships, active/inactive provider
accounts, multiple ordered streams and XC archive flags. Query cases include
plain/profile aliases, absent profile HEAD versus GET, POST bytes/CSRF, invalid
Bearer ignored on anonymous export, XC credentials/network/throttle, cached and
direct logos, tvg/gracenote selection, output suffixes, repeated query values,
Unicode/reserved characters, trusted/untrusted forwarding and two-second cache
staleness/events. Keep origin fixed through a fixture Host header so exact body
hashes compare without erasing URL-format differences.

## Current scoped evidence

`verified-m3u-host.json` records43 original/native host HTTP cases, exact export
and HTML bytes, original Django reads of native string cache values, and equal
27-download/4-blocked event counts. Source error JSON objects use the existing
semantic comparison normalization; each Content-Length still matches its own
body. Native SQL/renderer independently matches26 positive body hashes and
rejects channel/metadata capacity overflow. This is not a frozen application
route or full-M7 acceptance; concurrency/shutdown, cache expiry, provider
transforms, broad inputs, event hooks and resource gates remain open.

The initial Source38 capture remains at artifacts/rust/m7-m3u-source-1. A later
43-case capture completed requests but its license-selection helper mistakenly
included GIS dependency licenses; preserve m7-m3u-source-2 as a tooling failure.
The fixed capture selects Django's distribution license and retains it in the
output crate. Valid CSRF cookies/tokens allow POST empty/literal `{}`; a spaced
body is forbidden by the generator. Invalid tokens produce the original1004-byte
page; no-cookie failures produce1367 bytes. Actual Source evidence, not Django
unit Client defaults, defines those method contracts.

The latest host slice expands to50 cases, including actual profile names
`Kids & News`, `Plus+News`, `Café 東京`, and `Percent%26News`. Native/source
export and header bytes match, along with33 download/4 blocked DB events.
The SQL renderer independently matches30 positive hashes and refuses two
capacity overflows. Response actors do not queue data before body demand;
actual fixture tests verify held/unpolled/drop/shutdown recovery, with two
deterministic stop-subscription race tests and a path-decoding regression test.
The2-second stale-cache/expiry and Django reads of native cache strings pass.
The native router/shutdown is app-wired and waiting for its frozen-image gate.

After source capture, run the frozen application comparison using the common
runner with `--m3u-output --candidate-overlay <overlay.yml>
--candidate-build-manifest <build.json>`. The50-case oracle and all tool bytes
are copied before setup. This is an M3U slice only; larger exports, nonempty
provider credential transforms, broader path/CSRF/malformed cases, integration
hooks and other output protocols remain open.

## Multi-megabyte catalogs and resource limits

The original application's actual 6,000-channel fixture exports **3,492,158
UTF-8 bytes**. Its Django Redis value is a 3,492,167-byte pickle string; cold,
warm and expired-cache exports have identical hashes. The native adapter now
supports this fixture without truncation and original Django can read its
cache entry, write that string back, and have Rust read Django's encoding.
`--m3u-large-output` selects this separate catalog gate; add `--m3u-rust-http`
and/or `--m3u-rust-renderer` for actual native SQL/HTTP comparisons. It uses the
same disposable Compose runner and copied/hashed tools as the50-case gate.

| Setting | Default | Supported ceiling |
| --- | ---: | ---: |
| `RUST_OUTPUT_EXPORT_BYTES` |8MiB |64MiB |
| `RUST_OUTPUT_METADATA_BYTES` |8MiB |32MiB |
| `RUST_OUTPUT_MEMORY_BYTES` |192MiB |512MiB |
| `RUST_MAX_OUTPUT_REQUESTS` |4 |128 |
| `RUST_MAX_OUTPUT_CHANNELS` |50,000 |50,000 |

Limits are per gateway instance and shared by all its request/body tasks.
The memory budget independently admits a cold build only when it can reserve
`2*metadata + 6*export + 2*incoming-body + 1MiB` for metadata growth, output/cache
copies and scratch space. A configuration whose largest build cannot fit is
rejected at startup. Once building/caching finishes, admission shrinks to the
retained body allocation, all potential response-frame copies and64KiB actor
margin. Slow or unpolled bodies keep that charge. Outstanding emitted frames
retain the shared guard even if the producer finishes. Redis operations also
carry the guard through the cancellation-safe transport until their actual
reply drains, including when the initiating request is cancelled. No payload
is queued before body demand. Shutdown cancels unpolled body actors.

This budget covers admitted output payload/metadata/cache buffers; it is not
an RSS promise for allocator overhead, SQL/auth pools, HTTP/TLS stacks or Redis
server memory. Redis value lengths are checked before GET allocation, and each
content value is bounded by the configured export limit plus pickle overhead.
The string-only codec accepts the measured Django protocol4/5 string forms;
it cannot execute objects/imports/reducers. The general Store/auth cache codec
keeps its existing1MiB limit. Remote cache entries retain the original2-second
TTL and namespace; aggregate Redis/deployment resource acceptance remains open.

Exceeding catalog, metadata, cache-value or shared admission limits returns an
explicit503 rather than a truncated playlist. Larger valid original exports
outside configured/hard bounds remain unsupported; they do not establish full
M7 parity. Nonempty provider-transform allowlists, XMLTV/Xtream/HDHomeRun,
integration hooks and frozen-app/resource acceptance remain separate gates.
