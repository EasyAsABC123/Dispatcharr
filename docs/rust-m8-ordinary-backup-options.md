# Ordinary backup OPTIONS metadata

Pinned Original and Rust `58bcb6771f10`
matched all ten authorized backup metadata requests in the final
owned Compose replay. Both backends returned 200, `application/json`
and `Vary: Accept, origin`. The complete typed JSON matched, including
names, descriptions, ordered parsers and ordered renderers. Upload
reported multipart before form.

The eight added metadata routes cover create, upload, schedule,
schedule update, task status, download token, delete and restore.
List and download are the two existing controls. Each backend ran
ten OPTIONS requests with unchanged scoped SQL, broker and backup
directory states. Exact cleanup retired the four owned container
and process generations and left the project inventories empty.

In the first Original-only run, all ten `Allow` values put OPTIONS
first. The old draft's first Source gate exited 1 because it put
OPTIONS last. The corrected presenter agreed with that Original run.
The first strict paired run exited 1 before Rust started after
its fresh Original returned `GET, OPTIONS` for list, schedule,
status, download token and download. The next strict run admitted
those GET permutations and exited 1 before Rust started after
its fresh Original returned `POST, OPTIONS` for create, upload
and restore. Each attempt retired its two owned Source container
and process generations and left the project inventories empty.
Neither earlier host-only draft was packaged.

The pinned Original uses Django REST Framework function views that
form a Python set from OPTIONS and the declared route method. The
final replay therefore accepts exactly the two literal permutations
of each Original route's two expected methods. Rust uses canonical
OPTIONS-first order for all ten. Every other response field remains
an exact comparison. The receipt records the admitted permutations
and separately records the literal values returned by each actual
Original observation and the final Rust arm.

The exact Rust 1.90.0 workspace passed formatting, strict
all-target Clippy and workspace tests with 1,523
passed, 0 failed and 814 ignored.
The package and host checks matched 2,038 application inputs and
all 497 protected React and credit inputs.

This workflow covers authorized metadata. Backup jobs, restore and
comparative performance retain their separate gates.
[Redacted receipt](../scripts/rust-integration/m8-ordinary-backup-options.json)

The original Dispatcharr authors and contributors retain full credit.
[Attribution](../ATTRIBUTION.md)
