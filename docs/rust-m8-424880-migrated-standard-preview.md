# Migrated Standard-user browser Preview on 424880a9

The historical copied SQLite database bridged through historical PostgreSQL
and the Original upgrade. The ordinary Rust PostgreSQL package on
`424880a9` used the same imported Standard user and Channel. The copied
password hash was preserved. Original and Rust passed setup, anonymous
access, invalid and real credential checks, channel reads, and recurring
schedule reads. The separate failed bridge case preserved its rollback state.

The unchanged React UI signed in as the imported Standard user and opened
Preview in Chromium. The request returned HTTP 200 with the expected profile
query. Decoded video advanced from 25 to 75 frames. The owned origin saw one
connection. Closing Preview and stopping the fixture retired the browser,
relay, origin, and all 11 containers, one network, and one volume.

The first two attempts failed at the Native login probe and retired their
owned resources. The second run identified the fixture's new loopback-only
UI rule. The corrected fixture kept the inherited network defaults and
both backends accepted the real credentials. The third run preserved that
result, then hit the test origin's PID 1 shutdown behavior and retired its
resources with a forced stop. The final same-image run used a real SIGTERM
handler and verified normal exit from the origin and the full workflow.

This is a historical database migration into ordinary PostgreSQL Main.
Direct copied-SQLite native playback and formal release acceptance remain
open. No resource comparison ran for this scope.

[Machine-readable evidence](../scripts/rust-integration/m8-424880-migrated-standard-preview.json)

Original Dispatcharr and dependency authors retain full credit for the
application, models, algorithms, and protocols.
