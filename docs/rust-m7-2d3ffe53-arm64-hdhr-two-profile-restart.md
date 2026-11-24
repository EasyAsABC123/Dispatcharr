# M7 ARM64 configured HDHomeRun profiles across restart

Lappy ran the pinned Original backend at `1241b3f7` and the ordinary signed Rust package at `2d3ffe53` in separate disposable Compose projects. The fixture followed the configured HDHomeRun discovery, advertised lineup, and guide 2.5 URLs. The active profile kept its exact configured ID and produced MPEG-2 video and MP2 audio at 192×108. The inactive profile advertised a URL without a query and passed through H.264 video and AAC audio at 320×180. Each of the eight deliveries totaled 7,000 aligned MPEG-TS packets, decoded both tracks with at least 50 video frames, and used one upstream connection without fallback.

Both backends replayed the two profiles after a graceful stop and start. The application container was the same, its process generation changed, and the origin and database generations stayed the same. The eight tracked SQL tables were unchanged. An independent review verified the application and probe exit codes, HTTP discovery comparison, and retirement of seven containers, one network and two volumes per backend. [Redacted proof](../scripts/rust-integration/2d3ffe-arm64-hdhr-two-profile-restart.json)

The fixture used configured HTTP endpoints and a local media source. M7 still includes multicast discovery and hardware tuners, plus the wider output matrix.

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
