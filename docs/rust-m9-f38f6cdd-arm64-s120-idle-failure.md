# F38 ARM64 S120 working-set failure

The `f38f6cdd09ad214d3d3ff18b0a46c286637fc9d1` ordinary ARM64 package passed its signed startup and independent package audit. The S120 campaign exited 0 and retained 25 active samples, 20 A/V captures and two guide outcomes. The independent S120 proof exited **1** at the unchanged **64 MiB** idle working-set growth gate. Exact cleanup passed and the engine was empty afterward. S360 and S7200 remain gated.

A read-only replay independently checked the two 11-sample windows and the reducer's original attribution. The whole-scope working-set median moved from **398,086,144 B to 488,333,312 B**, an increase of **90,247,168 B (86.066 MiB)**. That exceeds the threshold by **23,138,304 B (22.066 MiB)**.

| Role | Working-set median change | File charge median change | Anonymous median change |
| --- | ---: | ---: | ---: |
| Rust candidate | +84,402,176 B | +71,622,656 B | +13,103,104 B |
| Original and fixture data support | +4,022,272 B | +507,904 B | +3,801,088 B |
| Origin | +1,912,832 B | −71,081,984 B | +2,121,728 B |

The Rust candidate's active-file median rose by 70,664,192 B and its mapped-file counter remained zero. Origin's inactive-file median fell by 71,081,984 B. Candidate and Origin file counters each stayed flat across all 11 post-drain snapshots. The three per-role file median changes sum to +1,048,576 B. The whole-scope working set measures charged memory after subtracting inactive file cache from each cgroup.

The paired file-charge changes are consistent with a change in the page-cache charges or active/inactive accounting. These counters cannot identify the backing files or show whether the same pages changed cgroups. They also show 13,103,104 B of candidate anonymous growth. The failed gate remains in force. [Numeric proof and exact attribution digests](rust-m9-f38f6cdd-arm64-s120-idle-failure.json)

Original Dispatcharr authors and contributors retain full credit.
