# Bounded late MPEG-TS reads on fe054fb4

Exact packaged Rust revision `fe054fb473fcb22101a56dfe1458638f998a3b2a`.

Across ten active channels, 100 total late reads matched historical
media prefixes without opening an additional upstream connection.
The fixture capped concurrent reads at 32. Four samples checked the
60-second history policy and the 4 MiB local and 16 MiB shared payload
ceilings per channel. Twenty captures decoded audio and video.

The workload and independent project cleanup both exited 0.

Full milestone coverage and comparative CPU and memory measurements remain open.

The original Dispatcharr authors and contributors retain full application and behavior credit.

[Redacted receipt](../scripts/rust-integration/m1-m2-mpegts-retention-fe054fb4.json)
