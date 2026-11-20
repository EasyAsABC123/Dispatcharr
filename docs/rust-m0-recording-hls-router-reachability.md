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

The router declaration still needs its own documented legitimate request
that resolves to it and an observed method and role response. The
register continues to show 797 of 798 HTTP declarations with partial
Original coverage and leaves this GET unmapped.
[Contract register](rust-contract-register.md)

The original Dispatcharr and Django authors retain full credit.
[Attribution](../ATTRIBUTION.md)
