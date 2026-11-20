# Ordinary shared MPEG-TS reconnect

The Rust Main entry point uses the Original Dispatcharr reconnect policy for registered direct-HTTP MPEG-TS playback. It waits 250 ms per consecutive failure, capped at 3 s, and resets the progression when it changes source or the failure window expires. Shutdown and ownership-loss signals can interrupt the wait. Existing library constructors preserve their configured fixed retry delay, including an explicit 500 ms. Original Dispatcharr authors retain full credit for the application and reconnect behavior.

The Docker Compose fixture used pinned Original revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and immutable Native revision `53937faf4344bebc2ae6cb94c7130bf6af513da5` (image ID `sha256:8ca51c50481d23d46da6f6f24a2d55f212e9ad115fc53dce7c1095b608e19b0d`). Each phase held two existing HTTP 200 MPEG-TS subscriptions across a primary cut, two rejected same-source requests, and recovery on an alternate. The test used the guest origin's monotonic clock. Each retry age is the start of that request minus the previous attempt's terminal timestamp.

| Phase | First retry (ms) | Second retry (ms) | Alternate after second retry (ms) | Container generations retired |
| --- | ---: | ---: | ---: | ---: |
| Original 1 | 353.614 | 604.425 | 118.326 | 3 |
| Native 1 | 252.141 | 501.431 | 1.602 | 5 |
| Original 2 | 353.782 | 605.123 | 119.157 | 3 |
| Native 2 | 252.158 | 501.499 | 1.413 | 5 |

All four phases met the prescribed first- and second-retry windows of 225–450 ms and 475–750 ms. Every phase recorded four nonoverlapping origin requests with statuses 200, 503, 503, 200 and at most one active origin request. Both viewers shared one sampled provider slot before the cut and after recovery. Both decoded audio and video before the cut and after the alternate became active. All 16 strict decoder captures contained 74 video frames with audio, and both original client requests remained open. Every phase returned its origin, viewer and provider slot counts to zero and removed its recorded container generations, networks and volumes.

The exact C122 package completed formatting, the Main binary check, library and HTTP tests, all-target Clippy, image build and bound paired Compose. The previous candidate's workspace-format failure remains recorded. The C122 Compose receipt SHA-256 is `1a1d69da51e4141780d1317304854abf274c667d787981d581ce2cf9aa4641ff`. These are scoped functional observations. The alternate-arrival timing differs, and the fixture uses acceptance windows for the two retries. No application resource comparison or full M2 closure follows from this run.

[Exact paired reconnect receipt](../scripts/rust-integration/milestone-batch112/ts-reconnect-c122.json)
