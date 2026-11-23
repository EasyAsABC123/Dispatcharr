# d5034bf1 local warm rollback rehearsal

The ordinary AMD64 Rust image ran the one-write rehearsal on copied
PostgreSQL data. After one acknowledged Native write, the same signed
session returned to Original, where the HTTP and SQL checks agreed.
Four full Original observations matched. Exact teardown and the
independent reducer passed.

This scope establishes local recovery for the recorded application and
fixture. The release decision, rollout, and default switch remain open.
Original Dispatcharr and upstream authors retain full credit.

[Machine-readable qualification](../scripts/rust-integration/m10-m11-d5034bf1-local-warm-rollback.json)
