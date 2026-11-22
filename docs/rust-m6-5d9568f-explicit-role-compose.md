# Selected/Core startup, import and scheduling rehearsal

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

The ordinary `5d9568f` image ran one explicitly configured Selected/Core worker and one web-only peer in Compose. The worker installed six stock schedule rows, held the sole PostgreSQL scheduler lock, and finished startup Recovery and Version tasks. A real HTTP XMLTV import finished with its Index and Series descendants. One naturally due scan finished with its Groups child, and one naturally due cleanup completed.

Graceful retirement flushed the cleanup count from zero to one and released the lock. The same web peer stayed ready. The independent reducer verified eight broker task families, exact retirement of all six fixture roles, and settled command output.

Two earlier attempts captured a watched input being discovered and a durability check occurring before retirement. The passing physical run separated the API input from discovery and checked durability after retirement. Its first reducer rejected the fixed Rust origin labels on the API import and its children. The read-only recheck matched those labels to the frozen source and verified the remaining journal, role and cleanup assertions on the same evidence.

The observed topology uses explicit worker configuration and a web-only peer. Default worker activation, raw cross-backend Celery origin-label parity, and comparative resource use retain separate gates. [Machine-readable evidence](../scripts/rust-integration/m6-5d9568f-explicit-role-compose.json)
