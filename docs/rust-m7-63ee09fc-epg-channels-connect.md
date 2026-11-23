# Signed guide events and Connect delivery for 63ee09fc

The ordinary AMD64 Rust package and the pinned Original handled one real
signed EPG import with Administrator and Standard WebSocket subscribers
already connected. Both roles received the same three ordered event
frames on both backends. The comparison verified normal WebSocket
closes, Administrator reconnect, and no replay frames.

The same run checked worker completion and one delivery to an owned
webhook receiver, with durable Connect history and matching public
readback. Both Compose projects passed exact teardown. Wider Channels,
Connect, and M7 performance coverage remain open.

Original Dispatcharr and its upstream contributors retain full credit.
[Machine-readable paired qualification](../scripts/rust-integration/m7-63ee09fc-epg-channels-connect.json)
