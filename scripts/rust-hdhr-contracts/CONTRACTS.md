# HDHomeRun HTTP source inventory

Original Dispatcharr authors retain full credit; see
[ATTRIBUTION.md](../../ATTRIBUTION.md). This inventory is preparation, not a
native or packaged parity claim.

Source is `apps/hdhr/api_views.py`, its two URL namespaces, models/serializer,
`apps/m3u/utils.py:calculate_tuner_count`, effective channel overrides and the
shared original network/origin helpers. No UDP/broadcast implementation was
found in the inspected HDHR application or launcher; HTTP `discover.json` must
not be described as a new broadcast service.

The `/hdhr/` namespace includes bare discover/lineup/status, channel-profile,
output-profile and combined profile variants, device XML and device CRUD. The
`/api/hdhr/` alias includes bare endpoints and device CRUD only. Discovery
accepts arbitrary profile names without validating profile existence; lineup
returns an empty array for an unknown profile. Missing/inactive output profiles
fall back to pass-through lineup URLs. Discovery still includes path IDs in
its generated name/ID. Device XML always uses the original fixed values.

Bare APIs use AllowAny but presented authentication still runs. Their GET/HEAD
handlers enforce global `M3U_EPG` policy, without applying authenticated user
channel/adult/profile restrictions. OPTIONS does not call that GET-only network
check. Device CRUD list/retrieve require Standard+UI policy, mutations require
Admin+UI policy, and metadata OPTIONS defaults to Admin. HEAD on class-based
views follows GET, unlike the function-decorated TS/HLS routes.

Discovery uses the first stored device's name/ID, but ignores its stored tuner
count. Tuner count excludes profile ID1, includes active profiles only under
active accounts, chooses ten when any is unlimited, otherwise sums positive
limits, adds all custom streams and clamps to minimum one. Profile/UI flags do
not gate these routes.

Lineup uses effective name/number overrides, excludes base hidden-from-output
channels, honors enabled named-profile membership, sorts by effective number,
and skips null numbers. It emits exact GuideNumber/GuideName/URL/Guide_ID/Station
fields. Source origin handling honors forwarded values only for trusted peers;
XML interpolation remains original behavior. Source HTTP goldens will precede
native implementation and include auth/role ordering, network/cache changes,
headers/methods, device validation/uniqueness and lifecycle/resource bounds.

## Recorded component gate

`verified-hdhr-host-1.json` binds the frozen Rust sources/binary and 127 actual
original/native HTTP comparisons plus three real PostgreSQL resource tests.
Original limited/unlimited arithmetic is exercised with active and inactive
provider profiles, a disabled account, and a custom stream belonging to that
disabled account (27 limited and 31 unlimited in this isolated fixture).
Earlier failed runs remain in artifacts: source1 had a probe import collision;
native1 exposed incorrect original table prefixes and redirect headers; source3
attempted SQL NULL in a non-nullable settings column and was corrected to legal
legacy JSON maps. Source4/native3 passed117; native4 added the two tuner proofs. Native5 added reserved devices-profile
route precedence; native6 proved incremental serialization, allocation-capacity
accounting and long-Host limits; native7 added successful metadata GETs with
Accept:text/html (the original returns Django JSON/XML responses even after
BrowsableRenderer negotiation). All three resource regressions passed with 127
HTTP cases.

`image.py` is the next packaged gate: it verifies the immutable image/source
manifest, runs the same original/native HTTP comparisons, follows an emitted
lineup URL into native playback, decodes a complete audio/video prefix with the
pinned original FFmpeg, and verifies process shutdown/Compose cleanup. That
gate must pass before a packaged HDHR claim.

## Frozen image18 gate

`verified-hdhr-image18-1.json` binds image
`sha256:af8352ff444b847d6edaed51c340578c0650f02f9b4881ff611b7fba4af9c44b`
to source `0598101581932ac6a95c8a7f33e4ef2dc7ea804197f1dfc5942a45045166d502`.
All 127 original/native HTTP cases passed. Following a generated lineup URL
returned actual H264/audio TS, and a complete prefix decoded 74 video frames
with zero decoder stderr. Full live-cut ffprobe stderr is retained separately and is also empty in this run.
The app exited zero without OOM/drain warnings; Compose cleanup passed. This
is a packaged HDHR slice, not full M7 or whole-app acceptance/performance.
