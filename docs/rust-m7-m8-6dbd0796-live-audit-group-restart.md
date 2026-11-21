# Live Connect and Channel Groups restart on 6dbd0796

On the exact packaged revision `6dbd079641c967f7532213ba16d0855d4603e4da`, the first useful-media observation found one authenticated MPEG-TS Connect audit per backend with the subscription and integration disabled. Each served useful media through one origin connection, wrote no delivery record, and sent no webhook. With the plain webhook enabled, each backend wrote one audit, sent one webhook, and wrote one linked successful delivery record.

Both backends created, read, renamed and deleted a custom Channel Group through ordinary authenticated HTTP. After a serving restart on the same PostgreSQL process, a fresh login read back the updated group. Claim and unclaim also matched, and independent groups stayed unchanged. The receipt records differences in the Allow header for create, delete, deleted_read, fresh_login, login, put, unclaim. [Exact paired results and cleanup](../scripts/rust-integration/m7-m8-6dbd0796-live-audit-group-restart.json)

The first Group controller exited 1 at Native startup before making a business request. It treated Docker's omitted empty image command as a missing required field. The retry retained the image, entrypoint and workflow checks and accepted both encodings of an empty command.

Workspace formatting, strict Clippy and 1,497 enabled Cargo tests passed. The first test invocation exited 101 when the sandbox denied fixture sockets. The successful rerun used the same frozen source and allowed local loopback. The group write contracts still use PostgreSQL. Native SQLite compatibility, wider delivery transports and comparative performance remain open. Original Dispatcharr, Django, PostgreSQL and dependency contributors retain full credit. [Original attribution](../ATTRIBUTION.md)
