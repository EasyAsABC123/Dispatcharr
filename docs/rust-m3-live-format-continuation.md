# Live-format sharing and owner-loss evidence

Original Dispatcharr authors and contributors retain full application credit.

Two readers on distinct Main processes shared one fMP4 source, provider slot,
profile transform, and remux on the exact Candidate111 image. The follower
received 854,025 fresh bytes after the lead reader disconnected. The decoders
returned 74 and 75 H.264 frames with AAC and zero errors. Both registrations
and the provider slot retired with the owned processes and Compose resources.

A separate Original HLS fixture killed the exact active worker and kept the
same follower and infrastructure running. The existing playlist capability
returned 200 through the 35-second poll and 404 from the 45-second poll onward.
This gives the Native owner-loss test its Original behavior. A fresh
admission was outside this fixture's scope.

[Scoped live-format receipts](../scripts/rust-integration/milestone-batch111/live-format-continuation.json)

## Two ordinary FFmpeg output-profile viewers

Pinned Original and the exact f3b8 Rust package each served two authenticated
fMP4 readers through the stock stream route. On each backend the readers
shared one origin connection, one FFmpeg profile transform, and one remux.
After the lead reader closed, the surviving reader received 826,598 fresh
bytes on Original and 854,025 on Rust with the same process generations.

Both captures on each backend shared the init and six complete fragments.
All four captures decoded H.264 and AAC with no decoder errors.
Rust Main ran with selected jobs and Beat disabled and exited zero.
Both runs retired the exact media processes and left empty owned container,
network, and volume inventories.

The ordinary FFmpeg input profile, broader M3, and comparative performance
remain open.
[Ordinary FFmpeg output-profile receipt](../scripts/rust-integration/milestone-batch113/f3b8-ordinary-ffmpeg-output-profile.json)

## Integrated a785 FFmpeg output-profile run

A fresh two-viewer Compose comparison passed on packaged a785a2b7 and
pinned Original. Both authenticated clients per backend received HTTP 200
fMP4 and shared one upstream connection, one FFmpeg output-profile transform,
and one remux. After the lead viewer disconnected, the survivor received
826,598 fresh bytes on Original and 854,025 on Rust while
the same media process generations stayed active.

Both captures on each backend shared the initialization segment and complete
fragments. Independent parsing found 6 shared fragments on Original and
6 on Rust. All four captures decoded H.264 video and AAC audio with zero
decoder errors. Rust Main ran with the manual-backup owner, selected workers,
Beat and media diagnostics absent and exited 0. Both runs retired the exact
media processes and all five owned container and host generations. The project
inventories were empty.

[Redacted a785 receipt](../scripts/rust-integration/m3-ordinary-ffmpeg-output-profile-a785.json)
