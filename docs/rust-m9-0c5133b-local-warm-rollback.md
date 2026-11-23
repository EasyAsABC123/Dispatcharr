# Disposable warm rollback on 0c5133b

A quiescent Original donor produced two sibling databases for an isolated trial. The same signed loopback route went from Original to Rust, then back to the existing Original process. Rust acknowledged one synthetic channel rename, stopped cleanly, and released its database sessions before Original reconciled the change. Original kept its process and data-service generations and read the renamed channel through the same signed session and through SQL.

The full row comparison found the expected channel name and timestamp changes. Unrelated rows, schema, sequences and seeded media passed the fixture checks. Original confirmed one reconciled write. Donor, control and trial resources retired exactly. Preflight, run, proof and campaign all exited 0. The [aggregate receipt](../scripts/rust-integration/m9-0c5133b-local-warm-rollback.json) records the exact package and 7.712-second local fixture recovery interval.

This is a local rollback rehearsal with one acknowledged write. The measured interval applies to this disposable fixture. Broader concurrent activity, rollout monitoring, release-wide performance and a default switch remain separate requirements.

Original Dispatcharr, PostgreSQL, Redis, Django and dependency contributors retain full credit.
