# Connect fixed JSON negotiation

The ordinary Rust successor `bd07920` passed a fresh comparison with stock Original. Both `.json` spellings returned the exact 57-byte JSON 406 when the request accepted only HTML. The comparison also verified the JSON media type, absence of an authentication challenge, and the expected `Vary` values on that rejection.

Both spellings accepted JSON-compatible or absent Accept headers and preserved ordinary role admission. Each backend ran 15 owned GETs with exact teardown. Successful requests reached the same typed result. The wire record retains differences in successful response bytes or selected headers.

The `.api` browsable collection still returns 404 in Rust and 200 with HTML in Original. The [machine-readable comparison](../scripts/rust-integration/m5-bd07920-connect-json-accept.json) retains each response and cleanup receipt.
