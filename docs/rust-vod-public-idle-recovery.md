# Opt-in VOD idle recovery checkpoint

All application and algorithm credit belongs to the original Dispatcharr authors and contributors. This is a scoped Rust compatibility change; the React frontend is unchanged.

The checkpoint adds reopening an owned public VOD session after its active-stream count reaches zero. It preserves the original session identity and creation time, reads the current selected provider profile limit, reserves the provider, and increments the request count only after successful entry. A profile that becomes full returns the original 37-byte HTML `429` response without incrementing the request count or taking another movie's reservation. The playback route emits the captured single `Vary: Accept, origin` field.

Recovery remains **OFF by default**. This checkpoint does not close M4, enable recovery generally, or establish a performance gain.

## Exact tested scope

[The evidence summary](../scripts/rust-redis-restart-contracts/verified-vod-public-idle-293-298.json) binds the eight-file overlay to committed image44 source, producing689 Rust inputs. It records49 component controls, strict affected-package Clippy, two component MP4 decoder checks, and the exact private application image and build manifest.

Four actual Source/native Docker Compose comparisons cover successful reopening and a provider limit reduced from2 to1, separately for anonymous clients and clients that initially supplied a real Bearer token and then omitted it after Redis restarted. Each run uses the real SQL resolver, HTTP bodies, and distinct Redis server run IDs. All four comparisons preserve the candidate process. Sixteen bounded Source/native MP4 prefixes pass strict decoding. The other movie remains admitted and readable when the idle request receives429. Successful reopening survives the older cleanup timer.

The parent review checks every selected file hash, equality of all689 Rust inputs between the component tests and packaged image, unchanged frontend and attribution inputs, recomputes all four comparisons, and checks that their owned containers, networks, and volumes are gone. Authenticated and anonymous runs retain separate identities and reports.

## Physical ownership and remaining gates

Unique receipts and generation ownership prevent cleanup from decrementing a foreign provider reservation. Control and body admission remain held through actual claim, reservation, commit, and cleanup settlement after observer timeout or caller cancellation. Definite profile-full failures and ambiguous transport outcomes use separate cleanup paths; uncertain mutations are never retried as though they had failed before execution.

Concurrent followers while idle admission is pending remain unsupported. The original concurrent public-counter behavior captured in Source270 is not equivalent to an atomic private quota. Broader retired-generation, audit, and state-loss cases remain open. The retained raw success responses also show the original dynamic `X-Worker-ID` header and different connection-header values; those are explicitly excluded from this checkpoint's selected-header comparison. The allow-header check accepts only the two exact spellings observed in original processes.

The next M4 gates are those remaining concurrency and header domains, broader playback/session parity, whole-application integration, and source-bound throughput/CPU/memory measurements. Component checks and this private package do not replace the accepted image44 regression checkpoint.
