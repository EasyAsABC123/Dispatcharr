# Standalone backup restore after the archive parser refactor

Original Dispatcharr and dependency contributors retain full application and
workflow credit.

The exact `11455873` Rust package restored an archive made by pinned
Original in an independent PostgreSQL project. The signed HTTP result
and the dedicated Redis result matched the issued task. The same Rust
Main accepted the restored credential, denied the replaced credential,
and served the restored settings and channel. The separate control
database matched its initial state.

The standalone restore owner served this request with Selected Jobs and
Beat disabled. A create request returned 501 without publishing work.
The fixture verified retirement of both Original and all four Rust
Compose generations and confirmed empty project inventories.

The Rust change extracts the bounded PostgreSQL COPY row reader for
archived plugin metadata. Eight backup unit tests covered the parser
and existing restore guards. This physical run exercises the disabled
plugin policy. The enabled-plugin restore and release performance gates
remain open.

[Redacted standalone restore receipt](../scripts/rust-integration/milestone-batch113/114558-standalone-restore-parser.json)
