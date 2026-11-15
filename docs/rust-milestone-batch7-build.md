# Candidate7 production image

Original Dispatcharr and dependency authors retain all application, algorithm,
protocol and license credit. The React frontend is unchanged.

The [actual build](../scripts/rust-integration/milestone-batch7-build/build.json)
passes on committed runtime `52e8ebc7e4ce8b92368ec94797d60c0e44678687`, producing
image `sha256:62e4fb5ab2b06f98a6fbf26301d1ab4b8f9cb4161f3a89427d7c64f9ab700cd0`.
Its source-tree digest is
`7bf8f2fccc6a0b138cbbe67e726a4bdae76a153e33ce77616e247cf64407f94e`.

Physical verification matches all 1,678 archived files against Git: 1,181 Rust
inputs, 495 React inputs and two original credit/license files. Every Rust input
matches the [911-test SDK batch](rust-milestone-batch7.md). The production Rust
ELF hash is `4e3eb7302d39ead618ac9af76c6113554dbc3fe11777fee876db3b9f4a6c693e`;
its actual entrypoint runs as UID65532 from `/app`.

All 114 earlier React/original-credit hashes are unchanged. The probe also checks
Celery/Kombu notices and both original SQLx licenses, for 120 selected hashes.
Separate physical probes verify GPG 2.4.8 and 27 pinned crypto/trust/license files.
Every owned probe container is absent afterward. Exact commands, outputs and
exits are indexed in the
[inventory](../scripts/rust-integration/milestone-batch7-build/inventory.json).

The same image passes its [12-case PostgreSQL comparison](rust-batch7-tls-acceptance.md)
and [222-check selected worker cohort](rust-batch7-selected-m6-observations.md).
Those retain their exact functional scopes and fixture mount limitation. No
whole migration milestone or paired performance gain follows from this build.
