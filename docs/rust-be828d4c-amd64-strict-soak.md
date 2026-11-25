# BE82 AMD64 strict short and prefix soak

The ordinary BE82 package passed the 120-second run with ten signed viewers, 20 complete A/V decodes, and two Guide refresh terminals. The strict reducer checked every recorded role for swap and OOM. Median idle working memory grew by 5,881,856 bytes against a 64 MiB limit. Idle descriptors fell by 158 and sockets fell by two. The owner and independent cleanup both exited 0.

The 360-second run completed all 73 active samples, observed the Guide terminal at 300 seconds and retired viewer ownership. It failed the strict zero-swap gate. Original had 249,856 bytes of swap at the first pre-idle sample and at every sample thereafter. Rust, Origin and the relay recorded zero swap, and all measured roles recorded zero OOM. All idle growth limits passed. The strict gate stopped execution before the 20 per-clip decodes. Exact independent cleanup and the full engine inventory comparison both exited 0.

The [typed observations](rust-be828d4c-amd64-strict-soak.json) retain the actual phase exits, the raw-ledger checksums and the earlier D6 adverse evidence reference. The two-hour gate rejected admission. Further reproduction can measure baseline cgroup and process swap during pre-idle before investing in the full viewer window.

Original Dispatcharr and all upstream contributors retain full authorship and license credit.
