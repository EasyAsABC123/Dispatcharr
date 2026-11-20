# Two-viewer MPEG-TS sharing on packaged Rust

An administrator and a separate standard user authenticated and viewed the
same channel from separate Rust workers. The ordinary disconnect and
administrator stop cases each opened one upstream stream and occupied one
provider slot. After the first viewer closed, the second continued receiving
the stream through the same owner. A normal operator stop ended both streams.
All four captured responses decoded audio and video.

Compose ran the exact `a33cf4cc8af0a7753592fc7cca643a73d758c037` AMD64 package. Both the workload and
its independent cleanup observer exited with literal 0, and the owned
containers, networks and volumes were absent. The [redacted receipt](../scripts/rust-integration/m1-m2-mpegts-sharing-a33cf4cc.json)
binds the package, workload, literal exits and cleanup. Full M1, M2 and M9
remain open. The performance campaign remains separate.

The original Dispatcharr authors and contributors retain full credit.
