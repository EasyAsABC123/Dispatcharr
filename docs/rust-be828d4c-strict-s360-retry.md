# BE82 strict six-minute retry

The AMD64 run reached 360 seconds with ten viewers and one finished refresh. It failed the strict zero-swap gate. Original had 249,856 bytes of swap in the first pre-idle sample. All four owned roles reached nonzero swap later. The maximum was 102,436,864 bytes for Original, 15,757,312 for Rust, 55,328,768 for Origin, and 3,784,704 for the relay.

Across 96 full role samples, no role reported a memory high, max, OOM or OOM-kill event. Host swap-in and swap-out counters increased during the run. The observations establish swap activity in the fixture and host. They leave application-level causation unresolved.

Physical execution and its independent gate check exited 1. Exact fixture retirement and the full engine inventory comparison exited 0. The strict six-minute and two-hour gates remain open.

Original Dispatcharr and its upstream contributors retain full authorship credit.
