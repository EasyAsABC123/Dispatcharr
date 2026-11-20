# a33cf4cc Safe17 and credential hash upgrade

Two more required Compose nodes passed on the same a33cf4cc application
image, bringing its verified coverage to **6 of 127 required nodes**.

- Safe17 ran the shipped Rust CLI against the full 17-case Original corpus.
  The retained Original case capture supplied expected behavior. The Native
  execution and cleanup were fresh. Full Django CLI output parity remains open.
- The password hash comparison passed 34 checks across
  8 fixture credential variants, retired all
  8 fixture rows, and decoded
  75 media frames in the shared foundation.
  The two backends matched the observed sign-in result and persisted hash
  state for each fixture.

The scoped coordinator exited 2. Both nodes recorded execution, evaluation,
and independent cleanup exit 0. The exact raw coordinator report SHA-256 is
`178b711a6128cd0d1d1ab395208d2ef3e07d61bddff60e394a70bff6a2fce1fb`. The redacted supplement SHA-256 is
`5532f8cae19958b3a0463ea42dfc941da02d4f6bb95177a94bc92d8348b77ea6`.

The whole M9 matrix, soak, and comparative performance remain open. The
original Dispatcharr authors and dependency contributors retain full credit.
[Earlier a33cf4cc Compose coverage](rust-m9-a33cf4cc-coordinator-coverage.md) · [Supplemental receipt](../scripts/rust-integration/m9-a33cf4cc-safe17-auth-hash.json)
