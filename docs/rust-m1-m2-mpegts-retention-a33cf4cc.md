# Late MPEG-TS reads on packaged Rust

The isolated retention workload ran ten continuously active channels and
observed 100 late reads in total, with concurrency bounded to 32. Every late
1.5 MB prefix matched
the retained reference interval of 35 to 55 seconds behind live. Those reads
opened zero extra upstream connections. Twenty decoder captures confirmed actual
audio and video. Four observations verified the 60-second configured history,
4 MiB local payload ceiling and 16 MiB authoritative history ceiling per
channel. These are functional retention properties.

Compose ran the exact `a33cf4cc8af0a7753592fc7cca643a73d758c037` AMD64 package. Both the workload and
its independent cleanup observer exited with literal 0, and the owned
containers, networks and volumes were absent. The [redacted receipt](../scripts/rust-integration/m1-m2-mpegts-retention-a33cf4cc.json)
binds the package, workload, literal exits and cleanup. Full M1, M2 and M9
remain open. The performance campaign remains separate.

The original Dispatcharr authors and contributors retain full credit.
