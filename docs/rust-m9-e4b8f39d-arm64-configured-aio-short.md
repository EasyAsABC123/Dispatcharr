# ARM64 ten-viewer AIO replay with concurrent guide refreshes

The ordinary ARM64 Docker package for `e4b8f39d` served ten signed MPEG-TS viewers for the full 120-second window with one upstream connection, provider slot and encoder. All 25 cgroup samples and 20 independent H.264/AAC clip decodes passed. Both signed XMLTV refreshes returned 202 and preserved the same viewer sessions through the token exchange. Each refresh produced its own stored root and two successful child tasks, the expected programme and index, and empty owned queues and locks.

The measured application group used 11.179381 CPU core-seconds and peaked at 263,323,648 bytes of cgroup working memory. The median working set grew by 19,730,432 bytes across the eleven idle observations per side, below the 64 MiB limit. The idle role descriptor counts grew by two descriptors and two socket descriptors, processes stayed level, and thread count grew by one. Both idle windows kept the two-second sample schedule.

The fixture used ordinary AIO startup and disabled its sole official plugin repository in the isolated database before launch. This is a supported offline repository configuration. The default-enabled control accepted the guide HTTP request but left it at the Celery queue head after the startup plugin ETA task faulted the shared lane. The guide stayed idle and no terminal result or index appeared. The underlying plugin execution error is still unclassified. The separate configured control verified two successful complete guide task trees and healthy readiness while keeping startup active.

Physical preflight, run and owner exited zero. Exact Compose teardown retired four containers and the owned network's ID and name. The independent auditor initially required a named volume from a different fixture. The replay verified the retained Compose file had only host bind mounts, verified Rust Main's bind mount, and checked the precise four-container and two-network-identity roster and empty volume inventories. It passed against the original sealed receipts without another physical run.

[Configured short result](../scripts/rust-integration/m9-e4b8f39d-arm64-configured-aio-short.json) and [default and configured guide diagnostics](../scripts/rust-integration/m9-e4b8f39d-arm64-plugin-default-and-offline.json) record the earned scope. A matched Original performance comparison, the default plugin fault, and formal M9 remain open.

Original Dispatcharr authors and contributors retain full project credit.
