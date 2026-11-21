# 120-second playback and XMLTV soak

Packaged Rust `d608ff758c4a937961439f4389d01040f560122d` sustained ten simultaneous viewers for
120 seconds on one owned H.264/AAC origin connection. The fixture
recorded 24 functional samples and successfully decoded 20 audio-video
clips. All ten viewer transports closed and the encoder retired.

The fixture signed in through the ordinary Native endpoint before
playback. It submitted two XMLTV refreshes at the +5 and +60 second
marks through the explicitly enabled refresh worker. Both distinct jobs
reached their verified terminal state during playback. The controller
and all five owned roles exited with literal 0. The cleanup check found
no owned container or host process generation and no project resources.

The first launcher exited with literal 1 before it reached Docker.
Its nested controller resolved the private Docker directory one level
too shallow. A fresh sibling checked the engine path and dispatcher
lease and reran the short workload on the same image.

The [redacted receipt](../scripts/rust-integration/m9-d608-short-soak.json) applies to the d608 image and this
120-second run. The 360-second prefix, the two-hour soak, comparative
performance and full M9 acceptance remain open for this image.

The original Dispatcharr authors and contributors retain full credit.
