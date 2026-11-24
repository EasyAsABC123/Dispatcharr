# M4 fresh VOD Range parity

The ordinary AMD64 Rust package built from `2daeb067563627338ff767a9d63df105aea8c734` matched all seven selected direct VOD wire cases in Docker Compose against retained Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` evidence. Its build manifest SHA-256 is `5c9cdd497980b3f8cc3834ba7cc1eb80830d1e5be5d7cb1f22e2f888b57ef3c1` and the image is `sha256:185e6498c9ce19e5d99b8f62ada0847b7e1a60a3ab1e1e0635fb90addfe4cad4`.

| Fresh session request | Prior Rust package | Fixed Rust package and retained Original |
| --- | --- | --- |
| `Range: bytes=-256` | HTTP 206 with an empty response body | HTTP 206, 257 bytes from the start of the media |
| Explicit start beyond the end of the media | HTTP 500, 44-byte body | HTTP 416, 31-byte body |

Original treats an omitted start as zero in this VOD workflow. Rust now applies that suffix interpretation before opening the provider connection. It returns the existing 416 response when the provider confirms an unsatisfiable explicit range and reports the media length.

The comparison also covered HEAD, full GET, first and middle ranges, and a mismatched If-Range. The full MP4 passed strict video and audio decoding with 75 video frames. The package build, fixture, application stop and independent exact Compose teardown succeeded. The comparator used recorded Original HTTP evidence and issued no Original business HTTP requests in the replay. The bounded fixture tested direct VOD requests.

The VOD crate had 53 library tests pass, 23 ignored and zero failures. Three Redis-backed tests passed for the fresh Range regression, existing Python Range behavior, and interrupted-body resumption with exact cleanup. The private raw Compose contract SHA-256 is `b5e03ddc99545591456fd39b8defd5310508ba197f98c082fb6c958552281a68`.

The signed packaged VOD workflow, milestone-wide acceptance and successor performance measurements remain open. The published paired VOD measurements and the 25 physical rows on `3d8f9672` remain linked to their tested packages.

Original Dispatcharr and all upstream contributors retain full credit for their work. React, license, and attribution files are unchanged from the frozen package.
