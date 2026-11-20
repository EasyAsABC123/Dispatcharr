# c2a35f9d required coordinator coverage

Four separate Docker Compose runs verified 11 distinct required nodes on
the exact `c2a35f9d` production package. Every node passed execution,
semantic evaluation and independent cleanup. Each scoped coordinator
returned exit 2. The six-node run used a retained Original reference
for the 17 CLI cases, and executed the packaged Rust checks fresh.

| Required node | Observed work |
| --- | --- |
| Clean install | 11 checks without an Original or Django service |
| Safe operator CLI | 17 cases |
| Current programmes | 64 compared wire cases |
| Guide Grid | 43 compared wire cases |
| Credential hashes | 34 checks, 8 credential variants, 75 decoded frames, and 8 owned credential rows retired |
| Proxy authentication | 38 checks including 20 configured proxy checks, with 75 decoded frames |
| Shared MPEG-TS | 24 checks in three cases, with six audio/video decodes |
| Shared MPEG-TS after all replicas stopped | 14 checks and two audio/video decodes |
| Bounded MPEG-TS retention | Four samples across ten channels, 100 late readers, and 20 audio/video decodes |
| Full packaged Main | 1,024 checks across 18 suites, 75 decoded MPEG-TS frames, and matching Original and Rust HTTP 404 bodies for an invalid settings ID |
| Live React and Stats | Two browser arms, 112 identical shipped assets, advancing video, active-channel Stats HTTP 200, and ordinary player close |

These are 11 of the 127 required coordinator nodes. The other 116 lack
passing coordinator evidence on this image. The results leave the release
workflow, soak, rollback, and comparative CPU, memory and latency gates
open. [Redacted 11-node receipt](../scripts/rust-integration/milestone-batch113/c2a3-11-required-nodes.json)
[Earlier nine-node receipt](../scripts/rust-integration/milestone-batch113/c2a3-nine-required-nodes.json)
[Detailed MPEG-TS evidence](rust-m9-c2a3-functional-streaming.md)

The original Dispatcharr authors and contributors retain application and
workflow credit. [Dispatcharr upstream](https://github.com/Dispatcharr/Dispatcharr)
