# Recurring DVR rule collection

Original Dispatcharr authors and contributors retain full application credit.

The recurring rule GET and HEAD collection matched Original in both ordinary
and selected-job Rust Main. The exact Candidate112 fixture returned all 519
ordered rows with the expected response fields. Unknown query options and
pagination kept Original's behavior. The comparison covered nine caller
contexts and checked that recordings, rules, and Beat rows stayed unchanged.
All three owned Compose scopes cleaned up.

The implementation keeps the existing 4 MiB response limit. Rule creation,
worker execution, and the integrated DVR and performance gates have separate
acceptance scopes.

[Collection parity receipt](../scripts/rust-integration/milestone-batch112/dvr-recurring-collection.json)
