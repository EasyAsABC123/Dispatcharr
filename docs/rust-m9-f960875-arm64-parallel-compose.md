# Three independent ARM64 Compose lanes on Lappy

The original Dispatcharr authors and contributors retain full credit for the application. See [Attribution](../ATTRIBUTION.md).

On November 22, 2026, Lappy ran three Codex-prepared Compose fixture lanes on its local ARM64 Docker Desktop engine. The browser, output, and restore lanes used separate project names, networks, databases and volumes. The browser used one dynamically allocated loopback port. The output and restore probes ran inside their private networks.

The observer recorded 60 live samples, including five with all three project groups running together. It recorded no sampling error and no owned projects running at the end. Each fixture's own receipt verified exact cleanup.

| Lane | Exit | Evidence |
| --- | ---: | --- |
| Ordinary Native M3U/XMLTV | 0 | Authenticated `/get.php` and `/xmltv.php` produced one channel with matching IDs. Both missing-credential requests returned 401. The owned backup settings and schedule stayed unchanged. |
| Original React account browser | 1 | Installed Chromium passed the offline form and retirement check. The physical run failed at the browser UI checkpoint before it recorded execution. The Native browser arm remained pending. |
| Original disposable PostgreSQL restore | 1 | The Original counterpart failed and verified cleanup. The separately completed Native restore remains recorded in its [own receipt](rust-m8-f960875-arm64-standalone-restore.md). |

The [machine-readable receipt](../scripts/rust-integration/m9-f960875-arm64-parallel-compose.json) preserves the three literal outcomes and five overlapping observations. This was a functional concurrency experiment. All-port inspection, comparative performance and complete milestone acceptance remain open.
