# Two ordinary fMP4 viewers share one upstream

The pinned Original and the production Rust `fdede54c` image each served
two simultaneous authenticated fMP4 viewers from one upstream connection,
one transcode process and one remux process. Both viewers decoded audio
and video. The comparison verified at least six complete shared fragments
per backend and new media for the surviving viewer after the other viewer
closed normally. The independent checker verified exact owned cleanup.

This paired workflow is one part of M3. Full media-profile acceptance and
comparative performance remain open.
[Receipt](../scripts/rust-integration/m3-fdede54c-ordinary-fmp4.json)

The original Dispatcharr authors and contributors retain full credit.
