# Channel detail and collection OPTIONS metadata

Pinned Original and packaged Rust `fe054fb473fcb22101a56dfe1458638f998a3b2a` each served eight
authenticated administrator OPTIONS requests on copied PostgreSQL
with the unchanged frontend. The managed and manual detail responses
both returned 5,290 bytes of compact JSON. The detail `?format=json`
variant preserved those exact bytes and `indent=2` returned 8,011 bytes.
The unallocated detail ID returned 175 bytes without PUT actions.

Collection OPTIONS returned 5,287 bytes in compact JSON, also with
`?format=json`. The `indent=2` collection response returned 8,008
bytes. Its 31 ordered POST declarations and nine nested override
descriptors matched the detail PUT declarations. Rust shares one
static serializer contract for those representations.

All 16 responses returned HTTP 200. Each response matched its
Original counterpart's body digest, Allow, Vary, MIME and content
length. Seven database tables were unchanged within both runs and
across their handoff. Native used default Core with Selected, Beat,
EPG and standalone Version disabled. The fixture exited with literal 0
and verified all four retired roles and an empty project inventory.

The evidence covers the compact, query and indent-two wire profiles
and the compact unallocated detail. Browsable OPTIONS HTML, other
Accept parameter combinations and additional query shapes remain
unqualified. Full M0 and performance qualification remain open.

[Redacted paired receipt](../scripts/rust-integration/m0-channel-options-fe054fb4.json)

The original Dispatcharr authors and contributors retain full credit.
