# Active STD provider rename on default Rust Main

Pinned Original and packaged Rust `6e83d3dba839` each returned three HTTP 200
responses for an administrator's initial read, name-only PATCH and
readback of an existing active, idle STD provider. The typed response
preserved the record with only the provider name and nested profile
account name changed.

The linked four-hour schedule stayed enabled, kept its ID and interval,
and preserved the earlier last run and run count. The request advanced
the task's update timestamp and the Beat change marker. Both advances
fell within the observed request window. Unrelated owned SQL rows
stayed unchanged, and the readback added no write.

Original ran stock Celery prefork with Beat stopped. Packaged Rust Main
ran with Selected jobs, Selected Beat, EPG refresh and Version tasks
disabled. Each arm observed zero new broker admissions or stored task
results after the rename, preserved the preceding result keys, and
retired all its owned containers and observed process generations.
Rust Main exited 0.

The earlier inactive-provider comparison has its own evidence.
Full M6, the default durable worker graph and performance remain open.
[Redacted active pair receipt](../scripts/rust-job-contracts/verified-ordinary-active-provider-name-only.json)

Original Dispatcharr, Celery and Django Celery Beat contributors retain application and protocol credit. [Dispatcharr upstream](https://github.com/Dispatcharr/Dispatcharr)
