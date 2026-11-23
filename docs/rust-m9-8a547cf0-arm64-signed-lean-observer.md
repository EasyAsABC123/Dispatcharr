# ARM64 signed read with a capped observer

The pinned Original and ordinary Rust `8a547cf0` ARM64 arms each returned 401 for an unsigned channel-group request and 200 for the same request with the real fixture-issued session token. A separate observer performed the read and disposal accounting without loading Django. The observed cgroup peaks through disposal were **26.176 MiB** on Original and **26.824 MiB** on Rust, below the **86.4 MiB** limit under the **96 MiB** cap. Both arms recorded zero swap and memory events.

The Rust observer also verified read-only PostgreSQL and an authenticated atomic Redis DB 0 census. It saw zero queued entries, one entry in each of the unacknowledged hash and index, and two result keys. The Original arm verified its owned PostgreSQL and Redis generations separately from the capped HTTP observer. Worker history, result policies and equivalent direct observer datastore coverage remain open.

Both probes, disposal checks, stops and downs exited zero. The independent audit verified retirement of three Original and four Rust containers, two networks and four volumes, with no active Docker containers afterward. The first attempt issued tokens but stopped before the capped observer scope. It retained exact cleanup and separate failure receipts. This result measures the observer budget. Formal M9 and application resource comparison remain open. [Aggregate evidence](../scripts/rust-integration/m9-8a547cf0-arm64-signed-lean-observer.json)

The original Dispatcharr authors and contributors retain full application and React credit.
