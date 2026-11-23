# M7 ARM64 HDHomeRun discovery and output profiles

Pinned Original and Rust application `0c5133b62dc9f2df0900f2228e25b75baecb6754` passed nine configured HTTP cases per backend on Lappy. This is an independent follow-up to the [earlier configured discovery and lineup pair](rust-m7-0c5133b-arm64-hdhr-http-discovery.md). It used fresh Compose projects and recorded database-assigned IDs for the saved default, explicit active, and explicit inactive output profiles.

| Discover and follow case | Guide numbers from both backends | Profile used by the advertised stream URLs |
| --- | --- | --- |
| Standard and API discovery aliases | 2.5, 12 | Saved default |
| Named channel profile | 2.5 | Saved default |
| Named channel profile and explicit active output profile | 2.5 | Explicit active profile |
| Explicit inactive output profile | 2.5, 12 | Pass-through |
| Unassigned channel profile | Empty lineup | No stream URLs |

Each of the six discovery variants returned HTTP 200, and the client followed its advertised lineup and read its scan status. The two backends matched the typed discovery, ordered lineup, status, and selected headers. The fixtures verified the channel name and number override, hidden channel exclusion, and enabled membership. The API lineup alias matched the standard lineup. Both device XML aliases returned the same bytes across both backends.

The probe and application exited 0 in each arm. Both PostgreSQL and application process generations stayed stable, neither was observed OOM-killed, and all eight observed database tables and 184 migration records were unchanged. The independent audit verified exact absence of 12 containers, two networks, and four volumes. No host ports were published. This records configured HTTP behavior and advertised stream paths. Playback, transcoding, automatic LAN discovery, and comparative performance remain outside the fixture. Runtime observations include HTTP results and Docker process state. Application log and resource telemetry were outside the fixture.

The original Dispatcharr authors and contributors retain full credit. [Machine-readable evidence](../scripts/rust-integration/m7-0c5133b-arm64-hdhr-profile-discovery.json)
