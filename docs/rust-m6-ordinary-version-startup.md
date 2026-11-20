# Ordinary Version startup

The original Dispatcharr and Celery authors retain full application and
protocol credit. [Attribution](../ATTRIBUTION.md)

Ordinary Rust Main can publish the Version startup job when
`RUST_VERSION_TASKS_ENABLED=1` and `RUST_VERSION_STARTUP_OWNER=rust` are set.
This mode requires an explicitly assigned Celery broker and result backend
with one worker owner. The paired Compose run used separate owned namespaces
for Original and Rust. Selected jobs, selected Beat and EPG refresh were off
on Rust.

The retry exited 0 on pinned Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`
and exact Rust candidate `c74ea312bdc791ad12f5e43298b56bd9b81a19b5`, image
`sha256:01e513496e3ae02f8416ece3a07f2d5857e2998b5271f8138e9ed434a0685983`.

| Observed behavior | Original | Rust |
| --- | --- | --- |
| First startup | One unparented, no-argument Version job on `celery` | One |
| Terminal result | SUCCESS with JSON null | Same |
| Owned release endpoint | One completed HTTP 200 | One |
| Notice and event | One active, high-priority administrator notice for 0.34.0 and one event | Same |
| Guard | 300-second Redis key, expiry preserved across restart | Same |
| Sequential restart in the same cache | No second job, release request or notice event | Same |

Each first worker stopped normally before its replacement started. Both
replacement workers exited 0. The monitor recorded a guard attempt on both
starts in each backend. Redis reported the same absolute expiry before and
after each restart. The terminal result and notice rows stayed identical.
Each Compose run retired six tracked container and host generations and
left an empty project.

The first attempt exited 1 on Original. It recorded a root and SUCCESS/null,
with no release request or notice. Its worker inherited the baseline's
PostgreSQL socket path across a container boundary. The retry corrected the
Original worker to use PostgreSQL over the shared loopback network. The first
run and cleanup are preserved, and Rust did not start in that attempt.

This qualifies the observed startup and same-cache restart of the declared
Version worker. Daily scheduling, the shared default worker graph, full M6
and performance remain open.
[Redacted paired receipt](../scripts/rust-integration/milestone-batch113/ordinary-version-startup.json)
