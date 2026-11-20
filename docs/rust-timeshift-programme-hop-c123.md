# Same-session local Timeshift programme hop

The direct catch-up route can hand off an active archive to a different programme for the same authenticated viewer, channel, provider account/profile and provider stream. The request must supply the same explicit session ID. The handoff transfers the existing provider reservation under the archive owner token and generation. Original Dispatcharr authors retain full application and algorithm credit.

The isolated Compose replay used Original revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and immutable Rust revision `6ab6f5fce591547c091a8a9c1aa1df93912ccefd` with image ID `sha256:647ca1bbf0bfc8fd9926b213fa728ae71d6193834f32a563282f5e4ef106cf0c`. The verified Source-only run and both sides of the paired replay gave the following outcomes.

| Observation | Original | Rust |
| --- | --- | --- |
| Same route identity and changed public archive URL | Yes | Yes |
| First and second client responses | 200 and 200 | 200 and 200 |
| Bytes compared per response | 8,192 | 8,192 |
| Origin opens | 2 | 2 |
| Provider reservations before hop, after hop and after close | 1, 1, 0 | 1, 1, 0 |
| Guest exit | 0 | 0 |

All four compared prefixes had SHA-256 `9d83b91baa18ca8dbc1af53385360c52f6f0a167933ebddd220f9026f30e741f`. The paired run retired all five recorded container and host process generations, its network and its volume. Source-only retired all three recorded generations and its network. Both runs ended with empty resource inventories.

Package acceptance included workspace formatting, a Main binary check, directed archive library and upstream tests, all-target Clippy, the image build and the bound Compose pair. The two tracked JSON test fixtures temporarily supplied to Clippy were absent from the image archive. The approved Source-only receipt SHA-256 is `13a2c6dd2ef89eecc5d6c4233da6cd01e46309fbd637c1565d44f6e53173594e`. The paired receipt SHA-256 is `f8bfe60e4b85678177ccabbeca2f73fba38d453f335e90d8c69cfc8f734699c6` and the package manifest SHA-256 is `d13bbc4e907eb65d00a456d219dc1cc4e2a9f0933e8647ba5ceaa62bcc0c882b`.

Two earlier Source-only controllers exited 1. The first stopped while importing the frozen file-identity helper. The second captured the expected wire behavior and still failed its ownership accounting. The earlier package controllers exited 1 at formatting and then at Clippy, whose child exited 101 because two test fixtures were missing from the private build workspace. Those outcomes remain recorded.

This receipt covers one authenticated local hop with two origin opens. Cross-worker handoffs, cancellation races, resource performance and full M4 acceptance remain outside the measured scope.

[Exact paired programme-hop receipt](../scripts/rust-integration/milestone-batch112/timeshift-programme-hop-c123.json)
