# Channel JSON field order on fdede54c

The original Dispatcharr authors and contributors retain full credit.
[Attribution](../ATTRIBUTION.md)

Fresh Original and packaged Rust Main `fdede54c54ccb19300c01031537803a0b553f581` ran eight ordinary
Group Manager requests each. The manual and managed Channel detail
responses on both backends emitted the same 31 top-level fields in
the pinned Original order. The previously observed order differences are
resolved. The managed detail body matched byte for byte. All eight
responses matched the expected successful JSON and Allow headers, and
the typed business and PostgreSQL assertions passed.
[Earlier paired field diagnosis](../scripts/rust-integration/m5-2025258-group-manager-field-diagnostic.json)

Four raw body digests differed at the same known references to the newly
allocated renamed group. Those references appear in the rename response,
the renamed group detail, the manual channel's raw and effective group
IDs, and the group collection. The comparison treated that generated
fixture identity by its role and verified the other values unchanged.

The paired controller exited 0. All four exact owned role generations
retired and their project inventories were empty. The receipt is scoped
to this packaged workflow. Full M5 acceptance and comparative performance
remain open.
[Fresh paired receipt](../scripts/rust-integration/m5-fdede54c-channel-json-order.json)
