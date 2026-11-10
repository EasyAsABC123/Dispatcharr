# VOD setup ownership review

Original Dispatcharr authors retain full application and algorithm credit.

The next physical gate pauses only owned Redis for three seconds. Dropped Ticket cleanup must retain its true request/session permits past2.2seconds and settle after the actual replies, with no remaining session hash/provider count. A separate canceled initial setup holds the real EXISTS read, aborts only the caller, then checks the same permits before Redis resumes. These are physical ownership tests, not Source timing comparisons or performance tests.

Static review found that setup currently acquires permits inside the caller future. Cancellation during the initial EXISTS therefore drops both permits while a queued Redis read still exists. Later ADMIT has its own logical two-second deadline; PendingCreate cleanup is fenced but does not currently retain those two permits.156 records the actual counterexample before any fix.

The implemented narrow fix, with all21physical tests passing in coherent158 (overall158 still fails heartbeat accounting), is: acquire the bounded request permit before starting an owned setup task, clone immutable request/source/id into that task, and let caller cancellation drop only its JoinHandle. Carry shared request/session permits through PendingCreate's actual fenced cleanup response as well as the successful Ticket/Session. Admission remains bounded before task creation. Drain already checks request permits, pending cleanup count, local registry and coordinator idleness. No new unlimited background queue, replay or globally mutable request context is needed.

ClientStore has a separately observed post-restart failure in158: heartbeat Lua is loaded once and raw EVALSHA is cached. The current post158 fix performs bounded SCRIPT LOAD only after explicit NOSCRIPT and returns the failed flush error. It updates on a future flush without retrying the uncertain or partially completed pipeline;159 passes the fresh component gate, including unchanged8192-byte cumulative accounting/TTL on two independent later inspections. Read recovery alone does not establish heartbeat/byte-accounting recovery.
