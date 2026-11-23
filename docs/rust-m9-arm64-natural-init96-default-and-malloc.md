# Original ARM64 initialization at the 96 MiB observer cap

An isolated setup diagnostic ran fresh Original Django initialization twice, with default allocator settings and with process-local `PYTHONMALLOC=malloc`. Both observer cgroups reached 96 MiB and exceeded the unchanged 86.4 MiB headroom threshold. The workload used the same pinned Original source, settings, and ordinary callbacks, with fresh PostgreSQL and Redis support in each arm.

The default run completed all 29 configuration, model import, and ready callbacks. A later `ConnectionError` stopped the SQL and Redis verification. Both Redis support containers exited 1. The diagnostic did not establish which subsequent probe raised the error or why the cache exited.

The second run verified that CPython actually selected `malloc` at startup. Its setup process was killed by the memory controller during the open Channels ready callback. Its pulse exited 137. The end allocator check and complete callbacks were unobserved.

The pair exited 1. An independent audit verified teardown, including 18 exact absence checks. This startup diagnostic supplies no allocator benefit or formal observer acceptance. Original Dispatcharr and its contributors retain full credit.

[Setup and memory receipt](../scripts/rust-integration/m9-arm64-natural-init96-default-and-malloc.json)
