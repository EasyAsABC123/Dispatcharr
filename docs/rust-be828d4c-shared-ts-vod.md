# BE82 shared MPEG-TS and signed VOD evidence

All three shared MPEG-TS rows passed execution, independent evaluation, and exact cleanup. Signed viewers shared one upstream connection within each tested profile group. The all-down test recovered and cleared the old provider state in 6.09 seconds after restart. The retention test served 100 delayed readers across ten channels, decoded a late capture on each channel, and observed no additional upstream connection. Local and authoritative retention stayed within the 4 MiB and 16 MiB per-channel limits.

Both signed VOD scopes passed their checks and cleaned up exactly. The seven-case scope used fresh media for each cold request and verified a same-session warm suffix. For a cold unsatisfiable range, both Origins returned 416 and both services returned 500. The recorded Original and Rust error bodies were 123 and 44 bytes, respectively, so that error-body difference remains visible.

The [typed evidence](rust-be828d4c-shared-ts-vod.json) identifies the three additional canonical rows and the two scoped VOD results. The BE82 matrix now records seven verified package executions, two approved datastore authorities, and five retained Original authorities. The other 111 rows still require formal evidence.

Original Dispatcharr, FFmpeg, and all upstream contributors retain full authorship and license credit.
