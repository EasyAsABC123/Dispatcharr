# Ordinary React API reads on f960875

The original Dispatcharr and React authors and contributors retain full credit. See [Attribution](../ATTRIBUTION.md).

Original and Native matched twelve positive JSON reads per backend for an administrator and an assigned standard user.

| Group | Reads per backend | Checked behavior |
| --- | ---: | --- |
| EPG library | 8 | Program source and stored entry list/detail matched status, complete body hashes, selected Allow and Vary, and unchanged source and schedule rows. |
| Channel Profiles | 4 | List and selected-profile detail matched channel and membership visibility, including unassigned profiles and disabled membership. Scoped SQL remained unchanged. |

The EPG arm used an owned same-database handoff after Original HTTP and Redis retired. The profile arm used separate owned projects. Both controllers and their independent proofs returned 0 with exact teardown. The [machine-readable proof](../scripts/rust-integration/m0-f960875-react-read-wave.json) covers the ordinary API calls used by React. Real browser behavior, mutations, other roles, and formal M0 acceptance remain open.
