# Candidate55: provider profile PUT/PATCH

Original Dispatcharr, Django, DRF, Celery, Channels and dependency authors retain
all application, protocol and algorithm credit. React remains unchanged.

Candidate55 adds nested M3U account-profile PUT/PATCH after Candidate54's fresh
credential-profile batch reached a real PUT route returning 501. The six Rust
paths implement the existing serializer and model-save behavior, preserve
profile identity and capacity, and reuse the existing production notification
callback. One admitted SQL session owns the save, post-save effects and physical
close; an HTTP disconnect does not cancel that sequence. POST/DELETE remain
separate, unimplemented migration domains.

[SDK89](../scripts/rust-integration/milestone-batch55/SDK/index.json) passes all
four gates: formatting, workspace check, strict Clippy and workspace tests.
There are **1,198 passed tests, zero failures and 786 ignored tests** across 180
result blocks. All four commands completed with unchanged source inputs and
actual exit zero. Five added tests cover the retained default-profile request,
default field restrictions, nondefault PATCH patterns, XC expiration properties,
and positive integer bounds. Ignored tests are not behavioral acceptance.

The SDK records 1,458 total inputs, including 1,452 Rust-tree inputs. Delivery
contains 1,451 Rust-tree inputs: only the noncompiled patch backup
`rust/crates/store/src/auth.rs.orig` is excluded. Vendored `Cargo.toml.orig` files
are preserved. The [SDK summary](../scripts/rust-integration/milestone-batch55/SDK/summary.json)
records the exact tested hashes of all six implementation paths. Commit
`7c971c94853b4055158d65eca58a0725a78450c2` is published on the fork branch;
all 1,451 delivered Rust-tree inputs match SDK89 exactly.

The earlier attempts remain literal evidence. SDK87 failed to launch Cargo
because its relative toolchain-environment path resolved from the SDK Rust
directory. SDK88 returned zero for all four gates, but its patch application
skipped the five existing wiring paths. Its results therefore do not qualify
Candidate55's production profile wiring. Corrected SDK89 explicitly includes all
six paths and is the qualifying SDK batch.

The [Docker build](../scripts/rust-integration/milestone-batch55/build/index.json)
passes in 326.740 seconds as image
`sha256:ec3125bdb57a7e109b28c7be5f3c0b2beb3a7702ff523b7078d084f8762625dd`,
with source-tree SHA256
`55ab5a426e86830178bdb210d4ba6a71f17d57e63ba12f7afdccae83c8a86b4d`.
[All six package families](../scripts/rust-integration/milestone-batch55/package/inventory.json)
pass in 42.210 seconds, with 55 exact containers independently absent.
The [targeted PostgreSQL resolver regression](../scripts/rust-integration/milestone-batch55/resolver-PG/index.json)
passes in 49.663 seconds using SDK89 and the shared Cargo target. Root consumed
the actual direct command; all 24 owned child commands have EOF and settled
process groups, and its one exact container, original host generation and
project are independently absent. The outer command group was not separately
instrumented and is not claimed.

Fresh Native integration remains pending. The earlier package preflight exited
1 before starting containers because its copied tag guard still named
Candidate54; corrected tools2 runs all six families successfully. The complete
Candidate54 application batch subsequently exposed missing automatic recording
and Comskip result records, despite completed media effects. A focused diagnosis
is required before replaying that known failing long workflow on Candidate55.
Retained Source responses guide compatibility; they do not establish a fresh
Native pass. No Candidate55 CPU or memory gain has been measured. No complete
M0–M9 milestone or full Rust rewrite is accepted by these results.
