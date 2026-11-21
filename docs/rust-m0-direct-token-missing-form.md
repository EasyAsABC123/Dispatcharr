# Direct token missing-form response

The pinned Original returned 400 for one empty JSON form sent to `POST /api/accounts/token/`. It returned both required-field errors and `Allow: POST, OPTIONS`. The independently built Rust package `48686e07b2c3f51499d65d588c89560676cd066b` matched both observations from a fresh owned HTTP pair. [Machine-readable receipt](../scripts/rust-integration/m0-direct-token-missing-form.json)

Each request sent an empty JSON object to the fixed internal endpoint. A bounded account snapshot confirmed that the unrelated test account and other account rows stayed equal. The controller verified exact retirement of the owned roles. The scope covers this direct form-error response. The existing successful response, authentication logic, and unchanged frontend retained their pinned source inputs.

Full credit belongs to the original Dispatcharr authors, Django and Django REST Framework authors, and dependency contributors. [Original attribution](../ATTRIBUTION.md)
