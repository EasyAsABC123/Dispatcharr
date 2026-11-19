# Guide, XC, archive and operator checkpoint

The original Dispatcharr authors and contributors retain full credit for
the application and algorithms. The existing React build and attribution
remain in place.

- The packaged missing-group regression passed on both Original and Native
  Main. Each returned the same sparse 400 for missing IDs, and the subsequent
  GET and SQL comparison found no changes to memberships.
- The same packaged Native image passed the simultaneous first open of one
  archive SID across two distinct Main processes. One request returned media
  and the other returned the Original busy response. The provider reservation
  drained to zero. Both packaged regressions verified Compose cleanup.
- The guide probe compared 75 responses on the previous image. It identified
  four file-valued selector mismatches and one matching unrelated upload.
  The new Rust parser passes ordinary file selectors through the same type
  check as other scalar selectors. Filenames requiring normalization still
  use the existing guarded path.
- Repeated XC group entries now use the first submitted value, matching the
  pinned bulk-update behavior. The account PATCH used by React's Group Filter
  also accepts independent boolean preference changes while preserving other
  account settings and memberships.
- An explicit same-session archive tail request can use the active state held
  by a peer worker. The probe checks the active descriptor and admission,
  then revalidates the snapshot before contacting the origin. The real Redis
  media tests cover an exact 256-byte response, the retained owner and quota,
  and the busy response without a range.
- The packaged operator binary now shares Main's PostgreSQL configuration
  reader so it can use the Original `POSTGRES_*` environment variables.
  The comparison runner also accepts this configuration.

[Prior image guide control](../scripts/rust-integration/milestone-batch75/prior-image-guide-control.json)
· [Prior image XC](../scripts/rust-integration/milestone-batch75/prior-image-xc-references.json)
· [Prior image archive](../scripts/rust-integration/milestone-batch75/prior-image-archive-concurrent.json)
· [Focused Rust checks](../scripts/rust-integration/milestone-batch75/focused.json)

Packaged replay of the new paths and final release performance remain separate
gates in the [milestone ledger](rust-milestones.md).
