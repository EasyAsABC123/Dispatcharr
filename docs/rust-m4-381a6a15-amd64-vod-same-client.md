# AMD64 same-client VOD reconnect

Original Dispatcharr and Rust each served two complete MP4 reads to the
same signed client using one server-issued session URL per backend. Each
read returned HTTP 200, delivered 4,299,455 bytes, matched the synthetic
fixture's SHA-256 and completed strict FFmpeg video and audio decoding.
The preflight, physical run, cleanup, reducer and campaign all exited 0.

| Observation per backend | Physical result |
| --- | --- |
| Session issuance | HTTP 301 without opening an upstream connection |
| First playback | Request count 1 with one reader and one allocated provider slot |
| First EOF | Slot and active reader released, session metadata still idle |
| Second playback | Same URL and session creation marker, request count 2 |
| Second EOF | Slot and active reader released, session registry entry retired |
| Provider admissions | Two complete upstream requests, one per sequential read |

The controller used a private Compose network with no published ports and
ordinary default recovery disabled. It retired the Original web process and
cleared the fixture Redis state before starting Rust. Teardown verified the
exact baseline, origin, initialization and candidate container generations
and left empty scoped container, network and volume inventories. The guest
retained redacted proof and discarded the temporary MP4 files and decoder
output after checking both reads.

The Rust application was `381a6a157d1f860527906794bb8c63859fcc8176` on
Linux AMD64. Its ordinary image ID was
`sha256:fa7ef354649b47cd7255fc2970041ac9be74fb075608bbd43304b28cdc52a749`
and its verified build manifest SHA-256 was
`a501da5f41ddfede80b551d084d92767d97f8ff6faeeae64c3fce199f7af6de5`.
The pinned Original revision was `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.
This comparison covers sequential same-client playback during the normal
idle window. Concurrent sharing, interrupted-reader recovery, comparative
performance, and full M4 acceptance remain separate checks.
[Scoped evidence](../scripts/rust-integration/m4-381a6a15-amd64-vod-same-client.json)

The original Dispatcharr authors and contributors retain full application
and protocol credit. Existing dependency credits are preserved.
