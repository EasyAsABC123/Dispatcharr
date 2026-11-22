# Paired finite MPEG-TS handoff and fMP4 playback

Two local Compose comparisons exercised shared playback on earlier published
images. Each run verified its own source, release package, and cleanup.

The ordinary `4558a85b` image matched Original on finite MPEG-TS handoff.
Each backend served two authenticated viewers, retried the finite primary
three times, then requested one distinct fallback. The upstream peak was
one on each side. Four audio/video windows decoded per backend. Both
reported the fallback as active and kept the saved primary assignment.

The ordinary `25dc1502` image served two viewers through its FFmpeg fMP4
output profile with Rust dynamic health explicitly enabled. Each backend
opened one upstream connection, shared six complete fragments, and decoded
74 and 75 frames in the two player windows. The second viewer received
fresh media after the first disconnected. The Rust Main process exited
normally. Both comparisons verified exact owned resource cleanup.

The finite scope leaves the broader Main lifecycle workflow open. The fMP4
scope leaves the health timeout, expiry, drain, and settings-to-engine
workflow open. The formal matrix and comparative application performance
retain their previous status.

[Machine-readable evidence](../scripts/rust-integration/m2-m3-shared-playback-compose-followup.json)

Original Dispatcharr and dependency authors retain full credit for the
application, models, algorithms, and protocols.
