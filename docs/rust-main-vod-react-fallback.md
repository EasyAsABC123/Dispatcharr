# Main VOD React fallback

Malformed unencoded GET paths under /proxy/vod/ now reach the configured
bundled React index when the pinned Django URL converters do not match.
Classification happens before authentication or provider resolution. Valid VOD
routes, API routes and other methods retain their existing dispatch. Encoded
paths remain outside this correction's qualified domain.

The measured Original witness is one GET to
/proxy/vod/movie/not-a-uuid/owned-method-route in an explicitly declared build
of the unchanged Original React source. It returned 200 and 858 bytes. The
Original image's missing installed-template extraction remains a separate failure;
this build context does not establish equality to unobserved installed bytes.
The other route shapes follow the pinned URL configuration and converter witness.

The middleware streams the existing configured index in fixed 16 KiB chunks.
The response body owns its open file and pending reads; there is no new payload
cache or request-derived filesystem path. Eight tests exercise actual router
dispatch, malformed authentication headers, ranges/conditionals, missing files,
file ownership through response drop, and converter boundaries.

[Actual workspace verification](../scripts/rust-vod-contracts/react-fallback-qa/Root-review.json)
passes formatting, compilation, strict Clippy and 782 default tests, with 741
external-fixture tests ignored. The tested snapshot contains 856 Rust inputs and
five coupled inputs. A fresh immutable-image Main witness is still required.
Full M4, complete route/header/body parity and new performance gains remain open.
All Dispatcharr application and algorithm credit belongs to the original authors
and contributors.
