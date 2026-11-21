# Active catch-up playhead pause and resume

Pinned Original and the exact packaged Rust revision `9116a8cc54cebe988ab939de70e4f761d45a3db7` both completed a fresh authenticated catch-up session in Docker Compose. A live HTTP 206 response continued to deliver media while the viewer paused at 12.5 seconds and resumed at 18.25 seconds. Both position requests returned HTTP 204 and advanced the active session's observed playhead and pause state.

Each backend kept one upstream range request and one provider slot throughout the updates. Both returned the same 3,760,000 archive bytes, independently decoded audio and 74 video frames, and retired the session, provider slot, and all four owned service roles. The complete M4 performance and behavior gates remain open. [Redacted paired receipt](../scripts/rust-integration/m4-9116a8cc-active-catchup-position.json)

Original Dispatcharr authors and all dependency contributors retain full credit. [Attribution](../ATTRIBUTION.md)
