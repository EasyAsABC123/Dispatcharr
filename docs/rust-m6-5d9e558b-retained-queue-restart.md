# Retained queue head across Beat and worker restarts

The pinned Original published one naturally due backend cleanup task while its
ordinary worker was stopped. Identical raw queue bytes and task identity
survived a normal Beat stop and a replacement Beat loading the saved schedule.
The restarted worker consumed that task once and saved a SUCCESS result with a
null payload. The durable schedule advanced once, and the owned queues and
service generations retired.

In a separate Docker Compose run, exact Rust application
`5d9e558b70270e2dcc2b478d5ba96bca6a9abb8f` admitted the same pending Original
task through the existing Selected Main runtime. It used the same copied
PostgreSQL and Redis service generations, consumed the same task once, and
preserved its completed result after a second Rust Main started. Both Rust
generations exited normally. No duplicate of the completed task appeared.
Four exact container and host generations retired, and project inventories
were empty.

Two earlier Original attempts stopped at the test's live log observer. The
first expected a generic tick line the fixture did not print. The second
identified the correct saved schedule but could only see it after EOF.
The corrected observer flushed the small DEBUG stream while Beat was still
live. Native Compose exited 0. Its original reducer exited 1 because it
handled structured cleanup entries as strings. Independent reduction of the
same captured run exited 0.

This evidence covers Original-only pending restart and Original-to-Rust
handoff with a subsequent Rust restart after completion. A Rust-only
restart while a task remains pending, full M6, and performance remain open.
[Redacted receipts](../scripts/rust-integration/m6-5d9e558b-retained-queue-restart.json)

Original Dispatcharr, Django, Celery and django-celery-beat authors and
contributors retain full credit.
