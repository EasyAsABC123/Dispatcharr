# Native fMP4 transport under development

Original Dispatcharr authors and contributors receive full credit; see
[ATTRIBUTION.md](../../../ATTRIBUTION.md). AGPL-3.0-only is retained.

This crate is not enabled in the application. It compiles and passes strict
Clippy, but its remux process, HTTP flow and cleanup are still awaiting executable
integration checks. No M3 completion or application performance gain is claimed.

The implementation shares native TS input, registers real fMP4 clients, uses
fenced Redis output ownership, and invokes the original FFmpeg remux arguments.
It parses initialization and complete fragments into bounded output storage.
It includes the original AAC bitstream-filter fallback, initialization deadline,
byte-owner admission, and a single outstanding response-frame lease per viewer.
The framing component has separate Python differential evidence.

Next gates: real H264/AAC and non-AAC decoder checks, concurrent viewers and
replicas, late joining, init/fragment expiry, process death, cancellation,
backpressure, final-viewer stop, SIGTERM cleanup, runtime FFmpeg packaging,
settings defaults, app dispatch integration, and repeated paired performance.
