# c606 React live playback and Stats

Pinned Original and Rust revision `c6061adb` used identical copies of all 112
shipped React assets. In both Chromium sessions, the operator signed in
through the normal UI, previewed a channel and decoded advancing MPEG-TS
video at 320 × 180. The combined Stats request returned HTTP 200 with the
active channel, and the normal player control closed playback.

The scoped execution, evaluation and cleanup each exited 0. The independent
cleanup verified retirement of all four owned Compose roles, the frontend
asset donor and both Chromium process groups. The owned container, network
and volume inventories were empty. This run passed one of the coordinator's
127 required nodes and returned the overall partial-run exit code 2.

Original Dispatcharr authors and contributors retain full application and
interface credit. [Redacted browser receipt](../scripts/rust-integration/milestone-batch113/react-live-stats-c6061adb.json)
