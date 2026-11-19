# Account activity, HLS profiles and local playlists

The original Dispatcharr authors and contributors retain full credit for the
application and its algorithms. The existing React frontend is used for
acceptance.

- The XC account PATCH now accepts the boolean activity change sent by
  React's playlist toggle. It uses the existing transition, scheduling and
  expiry-notification paths.
- The live output-profile resolver now reads the Original integer spellings
  for explicit query values and saved preferences. The existing precedence
  and active-profile checks still select the profile.
- The local M3U refresh scanner now accepts the four default properties
  stored by the Original serializer, alongside the legacy empty object.
  File ownership, account shape and job checks retain their existing bounds.
- The paired XC fixture regenerates its Source archive and common driver
  from tracked Git objects. It verifies the exact candidate package and
  supports missing group, duplicate selection, preferences, orphan cleanup
  and activity checks. The activity assertions check schedule identity and
  expiration notifications as well as HTTP responses.

Twenty-five focused Rust tests passed and one fixture-bound test was skipped.
Strict Clippy passed on the touched crates. Nine offline XC fixture controls
passed. Candidate77 then passed the paired packaged XC activity and local
Refresh All comparisons. The two fixture setup attempts preceding
the refresh comparison remain in its receipt. The final comparison reused
the successful Original run after independently checking its cleanup.

The two-viewer HLS check passed with two equivalent profile spellings, one
origin connection, one selected transform, and identical decoded segments.
Candidate76 reproduced the fallback for the encoded spelling in the control.
The selected Native HLS owner and transform persisted after one viewer left.

[Exact-image Compose evidence](../scripts/rust-integration/milestone-batch77/packaged.json)

[Focused checks](../scripts/rust-integration/milestone-batch77/focused.json)
· [Portable XC fixture](../scripts/rust-integration/xc-group-patch/README.md)
· [Milestone ledger](rust-milestones.md)
