# Owned logos and VOD category reads on d426d322

The pinned Original and the exact Rust `d426d322` package matched seven
authenticated reads in disposable Compose projects. The comparison checked
linked channel-logo and VOD-logo metadata, both advertised versioned PNG
paths, a movie-only logo filter that excluded the series-only logo, and
separate ordered movie and series category lists.

Both PNG responses returned HTTP 200 and identical 68-byte image data,
with matching cache, modification, disposition, and content-type headers.
The typed JSON comparison normalized allocated IDs and the verified
local origin. The owned SQL and files stayed unchanged until teardown.
The controller and independent checker exited 0 and verified exact
retirement of two Original and four Rust fixture roles.

The previous `fdede54c` run exited 1. Original passed all seven cases.
Rust matched the two channel-logo cases and stopped at its first VOD logo
detail read, which did not satisfy the expected JSON 200 response. Both
projects verified cleanup. At that revision, the Rust route map included
the VOD image cache but lacked VOD logo metadata list and detail.

This supplement covers these seven reads. Broader artwork behavior,
formatted routes, mutations, formal required nodes and comparative
performance remain outside its scope.
[Machine-readable receipt](../scripts/rust-integration/m5-m4-d426d322-owned-assets.json)

Original Dispatcharr and dependency authors and contributors retain full credit.
