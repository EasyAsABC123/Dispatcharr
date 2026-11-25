# F38 ARM64 S360 PostgreSQL descriptor observation

A separate diagnostic companion ran the exact ordinary F38 package and retained the 360-second schedule, 73 active samples, 20 audio and video decodes, and the signed guide terminal. Physical execution, exact owned cleanup, and the diagnostic proof exited zero. The Docker engine was empty after cleanup. The unchanged formal proof exited 1 first at the 64 MiB idle working-memory limit. The two-hour soak remains gated. [Typed evidence and hashes](rust-m9-f38-arm64-baseline-postgres-descriptors.json)

| Idle median metric | Before | After | Change | Existing limit |
| --- | ---: | ---: | ---: | ---: |
| Working memory | 490,090,496 B | 593,076,224 B | +102,985,728 B (98.2 MiB) | +64 MiB |
| File descriptors | 268 | 497 | +229 | +16 |
| Socket descriptors | 152 | 62 | −90 | |

The independent file replay also calculated the descriptor growth from the two recorded 11-sample windows. The earlier S360 already failed at the descriptor gate. In this repeat, the formal checker stopped at memory and did not reach that gate. Candidate working memory grew by 83,234,816 bytes, with file charge up 71,622,656 bytes. Original baseline working memory grew by 17,936,384 bytes and Origin grew by 1,712,128 bytes. The role medians and whole-scope median difference use their own sample distributions.

The companion also classified descriptors for four stable PostgreSQL background processes in the Original baseline. Their regular-file total was 2 before viewing and at +210 seconds, 33 at +240, 137 at +295, 209 at +335, and 383 after formal idle. One stable background process accounted for all 381 additional regular-file descriptors. This reproduces growth before the +300 signed guide request. The exact PostgreSQL worker subtype, file families, and identity of the earlier attempt's process remain open. The companion grants zero formal soak credit.

Original Dispatcharr, PostgreSQL, and all other dependency authors retain full credit.
