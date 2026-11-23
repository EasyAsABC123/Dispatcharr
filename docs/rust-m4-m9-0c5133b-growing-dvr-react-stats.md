# Current growing DVR and React Stats acceptance

Both AMD64 scopes used candidate `0c5133b62dc9f2df0900f2228e25b75baecb6754`
and pinned Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

## M4 growing DVR

A paired Docker Compose fixture published two complete playlist
generations. The signed player saw both playlists and two exact MPEG-TS
segments with matching response metadata. Each independent decoder
returned 100 video frames with an audio stream and exited 0. The
reducer passed 24 recording checks and verified exact role retirement.
The fixture covers playback from an owned growing recording.

## M9 React playback and Stats

A fresh browser in each backend opened a normal channel preview,
decoded advancing video, and found the same active channel in the
Stats view. The browser closed the player normally, and all 112 shipped
React assets matched. The full `package/live-react-stats` node passed
execution, semantics and exact cleanup. The coordinator preserved its
literal exit 2 for the partial 127-node execution. This run qualifies
one required node. Full release acceptance remains open.

Original Dispatcharr and frontend authors and contributors retain full
credit. See the [attribution](../ATTRIBUTION.md) and [license](../LICENSE).

[Machine-readable evidence](../scripts/rust-integration/m4-m9-0c5133b-growing-dvr-react-stats.json)
