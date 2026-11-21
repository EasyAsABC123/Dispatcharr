# Channel Groups and token response headers on 1e0f875a

Original and Rust matched the `Allow` header on all ten ordinary Channel Groups requests and both successful direct token logins. The Group checks covered creation, reads, PATCH and PUT, claim and unclaim, deletion and the numeric 404 after deletion. A fresh admin login after each serving restart read back the edited Group, using the same owned PostgreSQL process within the paired test.

The scope uses an administrator and verifies exact Compose cleanup. Other roles, failed token responses and the login alias retain their own acceptance scopes. Rust formatting, strict Clippy and 1,500 enabled tests passed. Comparative performance and full M0/M5 remain open. [Exact paired receipt](../scripts/rust-integration/m0-m5-1e0f875a-group-token-allow.json)

Original Dispatcharr, Django and dependency contributors retain full credit. [Original attribution](../ATTRIBUTION.md)
