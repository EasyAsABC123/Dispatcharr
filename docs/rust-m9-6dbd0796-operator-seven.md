# Seven required operator workflows on 6dbd

The exact packaged Rust revision `6dbd079641c967f7532213ba16d0855d4603e4da` passed seven of the 127 required workflows in this batch. Both the final scope verifier and exact owned cleanup passed. The coordinator returned 2 because the full matrix remained incomplete.

- package/auth-hash
- package/packaged-cached-provider-info
- package/packaged-provider-actions
- package/provider-modal-browser
- package/shipped-createsuperuser22
- package/shipped-safe17
- package/strict-typed154

The first execution retained four workflow passes but the auth evaluator and cleanup callback failed after its underlying owned workflow exited 0. The interpreter alias reached a nested verifier that required the canonical executable path. The independent cleanup replay verified exact absence before the corrected batch started. Both attempts retain their literal exits. [Formal receipt](../scripts/rust-integration/m9-6dbd0796-operator-seven.json)

Comparative performance and full M9 acceptance remain open. Original Dispatcharr and dependency authors and contributors retain full credit. [Original attribution](../ATTRIBUTION.md)
