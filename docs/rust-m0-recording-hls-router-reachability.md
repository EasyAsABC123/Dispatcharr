# Original recording HLS route precedence

Pinned Original Dispatcharr lists the explicit recording HLS path before
appending the DRF router URLs. The explicit path has register ID
`route-0b392188d7e4e96171af` and uses the `<int:pk>` and
`<path:seg_path>` converters. The later
`route-37da04e4eae7ba9df918:GET` declaration comes from the
`RecordingViewSet.hls` action. Both declarations have the same route name.
[Original route order](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/apps/channels/api_urls.py#L54-L73)
[Original router action](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/apps/channels/api_views.py#L3850-L3862)

Django 6.0.9 resolves the first matching pattern. The integer converter
accepts decimal digits. The path converter accepts a nonempty remainder
that can contain a slash. The earlier explicit declaration therefore
matches ordinary numeric recording playback URLs before the router
declaration, even when the remainder ends in a slash.
[Django converters](https://github.com/django/django/blob/6.0.9/django/urls/converters.py#L5-L10)
[Django path converter](https://github.com/django/django/blob/6.0.9/django/urls/converters.py#L38-L44)
[Django resolver](https://github.com/django/django/blob/6.0.9/django/urls/resolvers.py#L628-L671)

The retained Original fixture used a generated numeric recording ID.
With redirects disabled, an authenticated administrator fetched its
playlist and segment with HTTP 200 and matched the expected headers
and bytes. The recording rows and sequence stayed intact. The paired
controller exited 0 and retired all owned resources. The retained
integration receipt SHA-256 is
`1168539f3f05403cdabb35c2313bd53280820dc87cfb230eeaa814680ed237f6`.
Those observations apply to the explicit route.

A fresh stock Compose fixture executed Django's resolver with three
decimal recording IDs across four path shapes. All **12 resolutions**
selected the explicit registration and captured the integer ID and entire
segment path. The fixture checked a playlist, a segment, a nested segment,
and a slash-ended playlist path. The plain DRF pattern also matched the
slash-ended examples, while the format-suffix pattern matched all 12
examples. The stock resolver selected the explicit registration first
every time. The three declarations use the same GET action and
authenticator definition. The witness ran zero HTTP requests, view
callbacks, and SQL statements. The controller and independent reduction
exited 0, and both owned container and process generations retired.
[Fresh resolver receipt](../scripts/rust-integration/m0-recording-hls-canonical-precedence.json)

The recording worker builds playback URLs from the generated integer
recording ID, and the playlist uses the same ID in its segment URLs. The
retained runtime snapshot places the explicit, plain DRF, and format-suffix
registrations at ordinals 42, 131, and 132. The plain DRF registration is
shadowed for application-issued decimal recording playback URLs. The
wider router identifier grammar remains outside this verified scope.
[Original recording URL producer](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/apps/channels/tasks.py#L2196-L2204)

The four historical framework cases attached to the format-suffix
registration by static request sites use the ordinary numeric playlist
and segment paths. Their request observations remain valid, with the
explicit route supplying the dispatch for those paths. The register
retained all 798 declaration identities and 797 partial Source assignments
at the earlier resolver checkpoint. This result qualifies the plain
GET's shadowing disposition for the issued URL shape. Native already has a
single playback route for that shape. The evidence leaves whole M0
acceptance open.
[Contract register](rust-contract-register.md)

## Authenticated missing-recording requests

A later pinned Original fixture checked fresh Django resolution and
six authenticated GETs through the owned stock HTTP listener. The
administrator's recording-list request passed first. Recording and
actor rows remained unchanged. The controller and strict reducer
exited 0, and both owned role generations retired.

| Missing ID case | First registration | HTTP status | Media type |
| --- | --- | --- | --- |
| canonical missing | explicit | 404 | application/json |
| zero padded missing | explicit | 404 | application/json |
| alphabetic missing | plain DRF | 500 | text/html |
| signed positive missing | plain DRF | 404 | application/json |
| signed negative missing | plain DRF | 404 | application/json |
| unicode decimal missing | plain DRF | 404 | application/json |

Only the four plain DRF observations attach to the new work item.
The numeric and recording-list controls remain in the receipt. The
register now has 798 partial Original assignments. Role coverage,
Native parity and full M0 remain open.
[Fresh stock HTTP receipt](../scripts/rust-integration/m0-recording-hls-plain-get-original.json)

The original Dispatcharr and Django authors retain full credit.
[Attribution](../ATTRIBUTION.md)
