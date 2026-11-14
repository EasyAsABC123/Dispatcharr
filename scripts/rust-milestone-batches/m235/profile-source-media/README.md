# Original custom FFmpeg media observations

All application credit belongs to Dispatcharr’s original authors and contributors.

This new Source context captured three fixed cases against the pinned Original
image, with two viewers across two replicas. No prior model18 or HEAD/credential
vectors were replayed, and no adaptive fallback was used. Both viewers received
HTTP200 MPEG-TS; each of six samples decoded 74 frames with audio and video.
Every context observed one actual FFmpeg process, one upstream connection, two
clients and one provider slot. Reader loss preserved the owner. Global stop
returned the actual expected200 response and retired resources.

Owner crash closed the upstream and left no matching FFmpeg process, but after
the fixed90-second window the Original provider reservation and input index
remained. This is captured Source behavior, not a passing cleanup assertion.
The final Compose teardown and five independent absence checks passed.

`publication-summary.json` preserves the literal raw `passed:false` marker:
this controller characterizes Source and does not grant full M3 acceptance.
Its exit0 means the three observations and mandatory cleanup were captured.
The exact raw result and executed tooling closure are hash-bound. The two Python
files are byte copies of the executed controller/helper. Compose/support files
remain referenced existing inputs; the Original tree and media are not copied.

This establishes Source media authority only for custom FFmpeg input over
MPEG-TS in the declared topology. VLC, output profiles, provider transforms and
custom-input HLS/fMP4 remain open. No CPU or memory gain is claimed, and retirement
wall times are observations rather than a controlled performance benchmark.
