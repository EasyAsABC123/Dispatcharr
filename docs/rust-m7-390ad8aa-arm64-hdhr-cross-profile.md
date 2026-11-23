# M7 ARM64 HDHomeRun explicit profile discovery and lineup

Pinned Original Dispatcharr and Rust application `390ad8aa16f07de7cbc7364a86f1eb2254faa4c8` passed six requests per backend in isolated Docker Compose projects on Lappy. The client read discovery, followed its advertised lineup, and checked scan status for two combinations of channel and output profiles. Both combinations extend the earlier ARM64 coverage. [Earlier ARM64 HDHomeRun profile pair](rust-m7-0c5133b-arm64-hdhr-profile-discovery.md)

| Request prefix | Guide numbers in the advertised lineup | Advertised stream URL treatment |
| --- | --- | --- |
| `/hdhr/output_profile/4` | 2.5, 12 | Explicit active profile 4 took precedence over saved default 3 |
| `/hdhr/owned-hdhr/output_profile/5` | 2.5 | Inactive profile 5 produced pass-through URLs with no output-profile query |

All six GETs per backend returned 200 with JSON and the same `Allow: GET, HEAD, OPTIONS` header. The device and tuner fields, advertised lineup addresses, and ordered channel entries matched. The scan-status JSON matched byte for byte at 84 bytes in both variants and carried the same `Content-Length` and `Vary: Accept, origin` headers. The fixture used a private network per backend without published host ports.

The independent audit on Lappy verified the package binding, exact case outcomes, unchanged scoped SQL across eight tables including all 184 applied migrations, and stable PostgreSQL and application generations. Preflight, run, proof, owner, and paired exit records each contained literal 0. The recorded teardown retired all 12 container IDs, both network IDs and names, and all four volumes, with empty project inventories. [ARM64 package evidence](rust-m10-390ad8aa-native-arm64.md) · [Sanitized pair receipt](../scripts/rust-integration/m7-390ad8aa-arm64-hdhr-cross-profile.json)

The scope covers HTTP discovery, lineup and scan status. Playback, transcoding, automatic LAN discovery, comparative performance and full M7 acceptance remain open.

Original Dispatcharr, React and dependency authors and contributors retain full credit.
