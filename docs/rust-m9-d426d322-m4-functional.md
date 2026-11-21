# M9 packaged VOD and Timeshift functional workflow on d426d322

The exact Rust package `d426d322b27a197aaaaa057ef73dfb53c516c452` passed the required `package/m4-functional` node. Both Original and Native ran the ordinary authenticated VOD and Timeshift paths. Each backend decoded 200 video frames with audio for VOD and 199 video frames with audio for Timeshift. The captured one-MiB origin prefix matched across backends for each format.

Both backends emitted the matching VOD start, Stats and stop event counts and the matching Timeshift Stats counts. Every playback phase released provider capacity. The canonical coordinator returned scoped exit 2. Execution, independent cleanup and evaluation each exited 0, and the separate reducer exited 0.

This adds one distinct required node to the same package. The workload used the functional playback mode. Full M4 parity and comparative resource and performance checks retain separate acceptance.

[Machine-readable receipt](../scripts/rust-integration/m9-d426d322-m4-functional.json)

Original Dispatcharr and dependency authors and contributors retain full credit.
