# Standard provider activation on default Rust Main

The unchanged React playlist table sends a single `is_active` field
when enabling a playlist. Pinned Original and packaged Rust `4faa831bcb6f`
each returned HTTP 200 on an administrator's initial read, literal
`{"is_active":true}` PATCH and readback for an owned disabled STD
provider. The typed response changed the active flag and status to idle.
The account name, profile, streams and channel links stayed intact.

The provider's existing four-hour task kept its ID and interval, became
enabled, and retained its zero run count. The task update timestamp
and Beat change marker advanced within the request window. The
subsequent read added no SQL change. Both arms recorded zero new broker
admissions or stored task results and kept their existing result keys.

Original ran its stock Celery prefork worker with Beat stopped. Packaged
Rust Main ran with Selected jobs, Selected Beat, EPG refresh and
Version tasks disabled and exited 0. Both Compose arms retired their
owned containers and observed process generations.

Deactivation, the full M6 worker graph and performance remain open.
[Redacted activation receipt](../scripts/rust-job-contracts/verified-ordinary-provider-activation.json)

Original Dispatcharr, Celery and Django Celery Beat contributors retain application and protocol credit.
[Dispatcharr upstream](https://github.com/Dispatcharr/Dispatcharr)
