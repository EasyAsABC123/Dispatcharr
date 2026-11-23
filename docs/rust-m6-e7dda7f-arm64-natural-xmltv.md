# Matching natural XMLTV jobs on native ARM64

Original `1241b3f7` and ordinary Rust `e7dda7f` each ran one authenticated XMLTV import in a separate isolated Lappy Compose project. Both used the same frozen guide bytes and returned HTTP 202. Each arm published exactly three broker tasks naturally, completed Refresh, Programme Index and Series Evaluation with matching parent relationships, and persisted the programme and index. Both arms drained their relevant queues and locks, kept the recordings table empty, and passed exact teardown.

Original used its stock entrypoint from the [verified ARM64 startup recipe](rust-m2-e7dda7f-arm64-startup-prerequisites.md). Rust used the exact native image and its ordinary AIO profile. Early Rust attempts exposed two backing-service setup gaps. The retry ran PostgreSQL as its pinned database account, verified a UTF8 cluster and Redis readiness, then added a SCRAM rule restricted to the disposable database and directly attached bridge. The successful runner added a peer authentication probe before starting the Rust app. The two failed attempts retain literal exits and exact cleanup in the [aggregate receipt](../scripts/rust-integration/m6-e7dda7f-arm64-natural-xmltv.json).

This qualifies the matching ARM64 natural job prerequisites with the exact revisions, guide and roles. The sustained ten-viewer playback comparison and its CPU and memory measurements are separate gates.

Original Dispatcharr, Django, Celery and dependency contributors retain full credit.
