# UTC time-only custom guide in Docker Compose

The original Dispatcharr authors and contributors retain full credit for the
application and custom guide workflow. [Attribution](../ATTRIBUTION.md)

Pinned Original and the shared packaged Rust candidate completed the UTC
time-only guide assertions on the XMLTV feed, programme grid, and Xtream short
EPG. Each interface had two observed phases on each backend. The assertions
covered both UTC kickoff times, event and filler clipping behavior,
microsecond window edges, and the capture order for logos and posters.

Both Compose backends exited zero. The Original fixture retired two
owned container and process generations, and the Rust fixture retired four.
Neither project had resources remaining. An earlier launcher stopped
before Docker because its private checkout omitted a coordinator helper. The
retry included that helper and preflighted the actual import.

This is a functional result for the named custom guide workflow. Full M7
acceptance and the deferred Auto Sync p95 comparison remain open.
[Validation receipt](../scripts/rust-integration/milestone-batch113/utc-guide-combined.json)

## Exact 735 repeat

The exact 735 package reran this workflow with fresh Original and Rust
fixtures. XMLTV, programme grid and Xtream short EPG each returned the
expected results for both UTC custom channels. Both fixtures verified the
kickoff times, clipped filler, microsecond grid boundaries, and logo and
poster interpolation order. Rust retired the four normal regex workers.

The paired controller and both backends exited 0. Original retired two
owned container and process generations and Rust retired four. All project
inventories were empty, and the separate UID probe retired. The earlier
stored-guide grid and 7adb results retain their original image bindings.
Full M7 acceptance and comparative performance remain open.

[Exact 735 repeat receipt](../scripts/rust-integration/milestone-batch113/735-utc-custom-guide-pair.json)
