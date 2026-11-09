# Native fMP4 transport under development

Original Dispatcharr authors and contributors receive full credit; see
[ATTRIBUTION.md](../../../ATTRIBUTION.md). AGPL-3.0-only is retained.

The application dispatcher is wired, and the runtime image now installs FFmpeg.
The new application image still needs its Compose media protocol gates. No M3
completion or application performance gain is claimed.

Six source-bound real Redis/loopback/FFmpeg tests pass across H264/AAC and
H264/AC3 fixtures. Both first and late viewers decode video and audio with at
least 50 frames; viewers share one upstream/remux. Final-viewer drop and explicit
shutdown reap the actual FFmpeg child, including while a body frame remains held.
The AC3 case exercises the original bitstream-filter fallback. A 100-viewer
fanout test with a retained slow frame passes under a 32 MiB read budget;
transient read admission applies bounded backpressure. See
[HTTP evidence](../../../scripts/rust-fmp4-parity/verified-http.json).

The implementation shares native TS input, registers real fMP4 clients, uses
fenced Redis output ownership, and invokes the original FFmpeg remux arguments.
It parses initialization and complete fragments into bounded output storage.
It includes the original AAC bitstream-filter fallback, initialization deadline,
byte-owner admission, and a single outstanding response-frame lease per viewer.
The framing component has separate Python differential evidence.

Next gates: Compose protocol/decode checks with the packaged FFmpeg, cross-replica
viewers, init/fragment expiry, process failure, cancellation and backpressure
stress, complete SIGTERM cleanup, dynamic settings, output profiles, and repeated
paired performance. FFmpeg package versions are recorded by the image build;
freezing all runtime package inputs remains a packaging gate.
