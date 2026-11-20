# Explicit VOD session identity

An explicit fresh VOD session ID now gets its own session when admitted. An exact existing ID still reuses its own session. The original Dispatcharr authors retain full credit for the application and its session behavior.

The combined Rust image `sha256:90bc93409ae1d7eb04985c6f208fc8e4c3f5d214be91e990f47611ad51eafd3f` at revision `7adb0c72dae360cc5c3a4b5e4d8f7b100e8f200b` passed the four event-relative Compose cohorts with controller exit 0. The comparison used pinned Original revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

| Playback request | Original | Rust |
| --- | --- | --- |
| Same explicit ID, two repeats | HTTP 200, same session, request count 3 | HTTP 200, same session, request count 3 |
| Fresh explicit ID, two repeats | HTTP 503, `No available stream`, no fresh session | HTTP 200, fresh session and its own request count of 1 |
| Stable nested `/proxy/vod/stats/` after fresh-ID dispatch | One unrelated connection, no fresh row | One unrelated connection and one fresh row |
| Provider slots at +100, +250 and +400 ms after last close | 2, 2, 2 | 1, 1, 1 |

Both same-ID responses and the unrelated playback prefixes matched across backends. All eight guests exited 0. Fourteen complete prefix decodes passed with audio and 75 video frames each. Every cohort retired its four recorded Compose container generations and left no container, network or volume in its project. Both named Redis/HTTP regression tests passed in the combined package and the exact Redis container and host process generation retired.

The earlier event-relative run observed Native incorrectly adopting the prior idle ID. The first C124 isolated test run exited 1. Its helper kept stdout but lost the panic stderr. The corrected test assertions preserve the outward compatibility range header, distinct response bytes and separate request counters. Both corrected component tests passed on the next attempt, whose controller still exited 1 because the cleanup helper rejected Docker's missing-container wording. The latest helper verifies removal with the exact-ID inventory and independently observes the absent host process generation. A separate diagnostic attempt stopped at socket access before launching a container. The failed receipts remain intact.

Provider-capacity release timing remains open. The Original 503 versus Rust 200 is still reproducible. This result establishes the explicit session identity and its visible stats behavior in the named fixture. It provides no performance, full VOD or full M4 acceptance. [Redacted physical evidence](rust-vod-explicit-session-id.json)

## Where the provider-slot counts first diverge

The first difference in all four cohorts appeared about 500 ms after the first shared reader's client socket closed. The second shared reader and the unrelated reader were still open. Original counted two readers on the shared session, Rust counted one, and both still held two provider slots. At +100, +250 and +400 ms after the second shared socket closed, Original still counted one shared reader and two slots. Rust counted zero shared readers and one slot. The unrelated reader stayed active, and the replacement request had not been dispatched. In the two fresh-ID cohorts, Original still counted one shared reader at about +900 ms and first showed zero at about +1,500 ms. [Redacted physical evidence](rust-vod-explicit-session-id.json)

Original handles both normal EOF and `GeneratorExit` by decrementing the active reader count, then releasing the provider slot when the last reader retires. Its one-second timer follows that release and governs session cleanup and stop notification. Rust's ordinary ticket cleanup also decrements the reader count and releases the last provider reservation before idle metadata cleanup. The samples locate the difference between client socket settlement and Original's server-side reader retirement. They do not show when the server closed its response iterator or establish a separate delay after the active count reached zero. [Pinned Original completion](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/apps/proxy/vod_proxy/multi_worker_connection_manager.py#L1544-L1596) · [Pinned Original slot release](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/apps/m3u/connection_pool.py#L427-L435) · [Exact Rust ticket cleanup](https://github.com/EasyAsABC123/Dispatcharr/blob/7adb0c72dae360cc5c3a4b5e4d8f7b100e8f200b/rust/crates/vod-http/src/sessions.rs#L1569-L1595)

### Bounded follow-up

Run one paired normal-client close case and one paired EOF case with the same pinned applications and deterministic paced asset. In the close case, repeat the two shared readers' orderly closes while keeping the unrelated reader open. Observe retirement before issuing another request. For the EOF control, give one reader the whole asset with no Range request and let it read to EOF before closing. Verify the full response byte count and hash. Cap each media observation at 180 seconds, then use the existing owned teardown.

Record origin transfer completion, client EOF and socket close independently. In Original, retain the existing stream-completed or client-disconnected log event and the provider-release log event. Relate those events to atomic Redis samples of active readers and provider slots using the fixture clock. In the single-reader EOF case, the origin's completed-byte counter can confirm a full transfer without attributing shared requests. If a log event or its clock alignment is missing, record the boundary as unobserved. This plan leaves the existing capacity timing gate open and adds no release delay.

### Observed client close and full-body EOF

Both normal-client Compose pairs passed with controller exit 0 on the same pinned Original and Rust images. All four guests exited 0 and their reader threads settled. Each pair retired four exact Compose generations and left an empty project. [Redacted observations](rust-vod-close-eof.json)

With the unrelated reader open, Original still showed two shared readers 750 ms after the first shared socket closed. Rust showed one by 50 ms. Both held two provider slots. After the second shared socket closed, Original showed one shared reader and two slots at 900 ms, then zero shared readers and one slot at 1,100 ms. The first Rust sample showed zero shared readers and two slots. At 50 ms it showed zero shared readers and one slot. These are sample times measured from local close settlement. The unrelated reader stayed active throughout, and neither client requested a replacement session.

In the full-body control, both clients received HTTP 200 with the same 4,299,455-byte asset and verified the full SHA-256. Each origin served exactly one complete media response. On both backends, the last Redis sample about 43 ms before client body EOF showed one active reader and one slot. The first sample about 132 ms after EOF showed zero for both. The separate origin counter first showed a complete response at about 133 ms after client EOF. Both clients reported return from the explicit HTTP close call within 0.04 ms of body EOF.

The Original log query completed with zero recognized completion, disconnect or slot-release markers. The observations bound reader retirement and origin completion to their sampling intervals. The order of Original response finalization, reader retirement and slot release within those intervals remains unobserved. Provider-capacity timing and performance remain open.

### Original completion markers at INFO

The Source-only follow-up repeated the ordinary close and full-body EOF
cases on pinned Original with stock `DISPATCHARR_LOG_LEVEL=INFO`. Both guests
exited 0 and each Compose scope retired both owned container generations.
The earlier paired run's WARNING setting suppressed these INFO markers.
[Redacted Source observations](rust-vod-stock-info-completion.json)

In the close case, Docker received two shared-session `GeneratorExit`
markers. The first arrived about 1,000 ms after the first client close
settled, around the second close. The second arrived about 1,010 ms after
the second close settled, followed about 0.73 ms later by the shared slot
release marker. The latter markers fell between Redis samples at +900 ms
with one shared reader and two slots, and +1,100 ms with zero shared readers
and one slot. The unrelated reader stayed open through that transition.
Both shared readers use the same session ID, so the logs identify the
session and the order of its markers.

In the EOF case, the client received all 4,299,455 bytes with the same
SHA-256. Docker received the normal completion marker about 0.13 ms after
client body EOF and the release marker about 1.01 ms after EOF. Redis showed
one active reader and one slot in the last pre-EOF sample at about −43 ms,
and zero for both in the first post-EOF sample at about +133 ms.

The marker times measure Docker console delivery. The fixture observed
Redis and origin counters separately. The elapsed interval from the client
close to Original's iterator finalization and the exact instant of the Redis
updates remain unmeasured. Provider-capacity timing and performance remain
open.
