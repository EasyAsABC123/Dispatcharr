# Backup Manager archive upload, download, and deletion on 585fde95

The original Dispatcharr and React authors and contributors retain full credit. See [Attribution](../ATTRIBUTION.md).

Pinned Original and ordinary Rust AMD64 `585fde95` matched the seven HTTP responses for a standard Backup Manager archive file lifecycle. Each isolated fixture administrator started from an empty inventory and uploaded a genuine PostgreSQL format-2 ZIP through the React multipart request. The inventory returned that file with the correct size and UTC modification time. The normal download-token route then issued a token, and opening its download URL returned the exact uploaded bytes and attachment headers. Both backends returned an empty 204 on DELETE, and a fresh inventory was empty.

| Step | Status on each backend |
| --- | ---: |
| Initial inventory | 200 |
| Multipart upload | 201 |
| Inventory after upload | 200 |
| Issue download token | 200 |
| Open signed download URL | 200 |
| Delete archive | 204 |
| Final inventory | 200 |

The Original archive was 38,980 bytes and the Rust arm's archive was 39,003 bytes. Each download matched its own uploaded fixture archive. Scoped account and backup-schedule SQL, queue and result observations, and the unrelated control file remained unchanged during the requests.

The run, cleanup, proof and campaign each exited 0. Independent cleanup confirmed both projects, all six recorded container identities and all five captured live process generations absent. The sixth container was the completed data-init role. The first attempt's probe exit 1, wrapper cleanup exit 1, and actual Compose cleanup exit 0 remain explicit. It stopped before Native when the fixture expected a wider token string than the normal response.

The [machine-readable receipt](../scripts/rust-integration/m8-585fde95-backup-file-lifecycle.json) records the exact image binding, route outcomes and teardown evidence. These archive file operations add scoped evidence for the release matrix backup-workflow row. Job creation, restore, schedules, retention, and full M8 acceptance have separate gates.
