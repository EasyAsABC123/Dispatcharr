# Saved Version and recurring recording jobs on 9cf6407

Original Dispatcharr authors and contributors retain full application credit. See [upstream attribution](../ATTRIBUTION.md).

Four saved Version cases used local HTTP 200 responses to exercise no update, update, repeat, and malformed version data. The exact Original and Rust runs matched the job results and notices and persisted one run for each case. The two Compose arms retired exactly.

Two naturally due occurrences of the same hourly recurring recording rule returned `1` then `0` in both backends. The first created one future booking and its linked artwork job. The repeat preserved that booking, and the schedule durably reached two runs. Both arms matched the linked task and recording notification outcomes and retired every owned role.

The tested Rust application was `9cf640701dacea07ea5f394ce7b18a6fa7c8c088`. Its workers used explicit Selected/Core ownership. The default graph, interrupted SQL checkpoint and comparative resources have separate gates. [Machine-readable evidence](../scripts/rust-integration/m6-9cf6407-version-recurring-compose.json)
