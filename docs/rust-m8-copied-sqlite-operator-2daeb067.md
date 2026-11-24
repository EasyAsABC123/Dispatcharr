# Detached copied SQLite migration verification

The five-case detached operator cohort ran from the exact ordinary AMD64 Rust package source `2daeb067563627338ff767a9d63df105aea8c734`. Its ordinary build manifest SHA-256 is `5c9cdd497980b3f8cc3834ba7cc1eb80830d1e5be5d7cb1f22e2f888b57ef3c1`. The opt-in companion was built with the copied SQLite server feature disabled and the detached operator enabled.

The default and opt-in help probes passed. Migration of a copied database produced 184 migration records and 62 application tables. A staged output collision and an unknown migration history both refused with exit 1. All cases ran with no network and user 65532:65532. The copied inputs retained their exact bytes through migration and both refusals. The application binary in the companion matched the ordinary package. The owner and independent verification both exited zero, and the Docker engine was clear of test containers afterward.

The detached cohort receipt SHA-256 is `6d85813d086ebc6a91e432b2b667f6285c8488379abe6d7f9e47e216b19fd6a6`. The [approved datastore scope](rust-m9-datastore-release-scope.md) uses PostgreSQL for the live service and this operator for copied SQLite data. This receipt applies to its exact application revision. The successor package requires its own receipt.

Original Dispatcharr and all upstream contributors retain full credit for their work.
