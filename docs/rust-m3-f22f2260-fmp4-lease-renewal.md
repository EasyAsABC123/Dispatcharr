# Profiled fMP4 viewer lease renewal

Exact Rust application `f22f2260dae5ae67293c785a54e558ea24103918` and
pinned Original passed a paired Compose playback check. Two authenticated
viewers on each backend decoded audio and video, and the surviving viewer
continued across the 60-second renewal point after its peer closed.

Each backend used one upstream and retained the same media generation.
Profile and remux ownership renewed by the final sample, at 62.247 seconds
for Original and 62.256 seconds for Rust. Original evidence also includes
renewal of the state and initialization records. Both runs decoded 74 and
75 video frames for the two viewers. The owned media processes, Docker
roles and network resources retired.

The fixture, independent reducer, and terminal owner exited 0. Full M3
acceptance and comparative performance remain open. Original Dispatcharr
authors and all media and runtime dependency contributors retain full
credit. [Redacted evidence](../scripts/rust-integration/m3-f22f2260-fmp4-lease-renewal.json)
