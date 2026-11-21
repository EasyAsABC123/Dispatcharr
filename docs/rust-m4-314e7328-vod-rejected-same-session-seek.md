# Rejected VOD same-session seek on 314e7328

Original Dispatcharr, Django, Requests, Redis and dependency authors retain
full credit. [Original attribution](../ATTRIBUTION.md)

The exact Rust image and pinned Original both returned HTTP 416 for the
known-end seek at 32,768 and 131,072 bytes per second. Neither started a new
upstream request. The public state stayed at two provider slots, one active
departing body and two requests before the next observed provider write. The
independent reader decoded 75 frames, the later ordinary request returned 200
and both Compose workloads verified exact cleanup.

This closes the scoped public-state difference recorded in the earlier
[c1ff diagnostic](rust-m4-c1ff-vod-rejected-end-seek.md). The provider timeline
does not establish when Native's local read settled. Full M4 and matched
performance remain open. [Exact paired evidence](../scripts/rust-integration/m4-314e7328-vod-rejected-same-session-seek.json)
