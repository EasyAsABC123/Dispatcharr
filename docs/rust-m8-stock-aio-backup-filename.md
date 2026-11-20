# Stock Original AIO backup downloads

Original Dispatcharr authors and contributors retain full application and deployment credit.

The stock Original AIO image at `1241b3f7` ran with its normal nginx and
uWSGI configuration, isolated tmpfs data and an explicit UTF-8 login locale.
The fixture copied a fixed 168-byte ZIP under four ordinary filenames.
It made 12 download and issuance calls through the same nginx listener.

| Filename | Admin download | Issuance | Signed download |
| --- | --- | --- | --- |
| ASCII | 200, exact ZIP | 200 | 200, exact ZIP |
| Latin-1 | 404 | 200 | 404 |
| Unicode Q | 404 | 200 | 404 |
| Unicode B | 404 | 200 | 404 |

Both ASCII downloads matched the entire ZIP, Content-Type, Content-Length and
Content-Disposition. All six non-ASCII downloads returned the same complete
162-byte error body. Content-Disposition matched the expected Django value on
all eight download responses. The internal redirect header stayed internal.
The archive copies stayed unchanged. All five controller phases exited 0 with
verified retirement of the exact owned app and empty project inventories.

The earlier paired streaming-fallback run on candidate `6667998f` exercised
three non-ASCII filenames with nginx acceleration disabled. Original and
Rust each returned HTTP 200 and the full archive for all six direct and
signed cases there. The stock nginx observations establish an Original
transport difference. Same-image Rust verification against this stock
authority, backup creation, restore, and broader M8 remain open.
[Earlier streaming-fallback receipt](../scripts/rust-integration/milestone-batch111/backup-filenames-and-original-sqlite.json)

[Stock Original scoped receipt](../scripts/rust-integration/milestone-batch113/stock-aio-backup-filename-source.json)
