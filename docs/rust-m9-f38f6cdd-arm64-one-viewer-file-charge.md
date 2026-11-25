# F38 ARM64 file charge at first useful payload

The audited ordinary `f38f6cdd09ad214d3d3ff18b0a46c286637fc9d1` binary served one signed viewer using the same finite MPEG-TS prefix and Origin pacing as the failed formal S120. The campaign and independent proof exited 0. One upstream connection and provider slot were observed. The viewer received useful audio and video, the encoder generation remained the same across the two live checks, four containers and two networks retired, and the engine was empty afterward.

| Checkpoint | Rust file charge | Rust active file | Rust file mapped | Origin file charge |
| --- | ---: | ---: | ---: | ---: |
| Before viewer | 258,048 B | 0 B | 0 B | 112,500,736 B |
| First useful A/V payload | 71,868,416 B | 70,668,288 B | 20,774,912 B | 1,703,936 B |
| Second live checkpoint | 71,868,416 B | 70,668,288 B | 20,774,912 B | 2,228,224 B |
| After confirmed drain | 71,868,416 B | 70,664,192 B | 0 B | 3,670,016 B |

The Rust file charge rose by **71,610,368 B** by the first useful payload and stayed at that value through drain. The same binary’s formal S120 recorded **71,622,656 B** of candidate file median growth and failed the original 64 MiB working-set gate. Origin file charge fell by **110,796,800 B** by first payload in the one-viewer run. The two changes have different magnitudes. The observations establish the first-payload timing and persistence of the Rust charge. They leave the responsible files and any relationship between the cgroups’ file charges unresolved.

The fixed-file probe opened and statted the Rust and FFmpeg binaries before the cache syscall denied their residency reads. The Origin mount was denied at open through its process root. The candidate had no matching Origin mount. The synthetic guide was the sole sampled file and had zero resident pages. The kernel checks per-file ownership, capability or write permission before disclosing residency. The physical receipt does not identify the exact permission predicate for either binary. [Linux cache syscall implementation](https://kernel.googlesource.com/pub/scm/linux/kernel/git/stable/linux-stable.git/%2B/refs/tags/v6.14.7/mm/filemap.c#4455)

A separate read-only replay found a constant 81,616,896 B native executable virtual mapping across all four checkpoints. The exact exported ELF has 81,611,212 file bytes in its loadable segments, including 78,908,996 executable bytes. The FFmpeg process appeared only during the two live checks. Its runtime mappings covered 74,645,504 virtual bytes and 74,084,352 B after deduplicating overlapping file ranges across 58 inodes. Virtual mappings and ELF load ranges describe addressable bytes. They do not identify resident pages or the cgroup charged for them.

The diagnostic changes no formal M9 or performance acceptance. S360 and S7200 remain gated by the failed same-binary S120. [Machine-readable witness](rust-m9-f38f6cdd-arm64-one-viewer-file-charge.json)

Original Dispatcharr authors and contributors retain full credit.
