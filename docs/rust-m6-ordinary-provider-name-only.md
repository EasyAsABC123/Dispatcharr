# Ordinary provider rename on default Rust Main

Pinned Original and packaged Rust `21337a5ff95b` each returned 200 for an
administrator's initial read, name-only edit and readback of an existing
inactive, unlocked STD provider. The complete typed edit and readback
matched the initial record with its top-level and nested profile
account names updated. The existing disabled four-hour schedule kept
its ID and zero run count. Its update timestamp fell within the request
window. Unrelated rows stayed unchanged, and the readback added no SQL
write.

Original ran its stock Celery prefork worker with Beat stopped. Rust
ran packaged Main with Selected jobs, Selected Beat, EPG refresh and
Version tasks disabled. Fresh observations in both Redis databases
found zero task admissions or result stores from the edit. Existing
result keys were preserved and the ready and reserved queues were
empty at both boundaries. The Rust Main process exited 0, and both
Compose arms retired their owned containers and observed host process
generations.

This comparison covers that one ordinary edit. The flags-off task
broker work, full M6 and performance verification remain open.
[Redacted paired receipt](../scripts/rust-job-contracts/verified-ordinary-provider-name-only.json)

Original Dispatcharr, Celery and Django Celery Beat contributors retain
application and protocol credit.
[Dispatcharr upstream](https://github.com/Dispatcharr/Dispatcharr)
