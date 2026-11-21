# Ordinary authenticated Timeshift archive playback on a33cf4cc

Pinned Original and packaged Rust Main each created one authenticated
Timeshift session with HTTP 201. After confirming the stored session
belonged to the fixture actor and channel, each opened its issued playback
URL once and read the full 1,880,000-byte HTTP 206 range. The response
digests matched. Each backend made one upstream request using the expected
colon-dash archive URL. The origin stayed in its default mode for both
requests.

Both range bodies passed strict audio and video decoding in the first
three seconds, with 74 video frames each. The first offline attempt to
decode the same retained byte range through EOF exited 183. The physical
comparison independently checked every advertised byte and used the
three-second window from the earlier qualified fixture.

Both sessions, media pools and client records retired naturally, and the
provider slot count returned to zero. The isolated Compose run exited 0,
with all four owned process and container generations absent and empty
container, network and volume inventories.

This receipt covers one ordinary default archive request per backend on
the exact a33cf4cc package. The [numeric and calendar VOD ranges](rust-m4-a33cf4cc-ordinary-seek-calendar.md)
retain their own scope. Full M4 acceptance and paired performance remain open.

The original Dispatcharr authors and contributors retain all application
and protocol credit. [Redacted receipt](../scripts/rust-integration/m4-a33cf4cc-ordinary-timeshift-archive.json)
