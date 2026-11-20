# Stock Original restore with one trusted taskless plugin

Original Dispatcharr and dependency contributors retain full application and workflow credit.

Two isolated projects used pinned Original `1241b3f7`. One created a
backup containing an enabled, independently trusted local plugin and
its saved settings. The other began with different settings for the
same trusted module. Ordinary upload and download preserved the archive
bytes.

A stock Celery task event matched the real restore worker generation
and the exact task UUID issued by HTTP 202. The signed status and the
dedicated result store returned the same successful result. The restored
business rows and plugin registry matched the archived state, and the
separate PostgreSQL control remained unchanged.

The same Original web process accepted the restored credential, denied
the replaced credential and served the restored settings and channel.
One ordinary plugin echo before restore and one after returned their
respective saved settings with defaults. Both matched their own live
plugin action markers.

The controller exited 0 after exact cleanup in both projects. This
covers one already trusted module without owned tasks. Migration receiver
execution and enabled-plugin restore in Rust remain separate.

[Redacted Original restore result](../scripts/rust-integration/milestone-batch113/original-trusted-taskless-plugin-restore.json)
