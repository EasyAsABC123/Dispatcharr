# Same-client VOD reconnect while session metadata is retained

Each backend issued one session URL to the same authenticated client.
The client used that URL for two sequential HTTP 200 reads. Each read
returned all 4,299,455 MP4 bytes, matched the expected digest, and passed
video and audio decode. Each backend made two requests to the owned
origin and completed both responses.

After the first EOF, provider occupancy and active readers reached zero.
The same session metadata was still present, with request count one.
The second GET reused that metadata and kept its original creation
marker. Request count advanced to two. Both active reads held the
single available provider slot. After the second EOF, the slot released
and the session metadata retired naturally.

The exact 11ddf3e2 package ran this ordinary workflow with optional
recovery disabled. The pair and its independent qualifier each exited
0. All four owned role generations retired and their Compose inventories
were empty. Full M4 and performance remain open.
[Redacted receipt](../scripts/rust-integration/m4-vod-same-client-reconnect-11ddf3e2.json)

All application and algorithm credit belongs to the original Dispatcharr authors and contributors.
[Attribution](../ATTRIBUTION.md)
