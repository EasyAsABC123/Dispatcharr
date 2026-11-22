# Connect delivery-log responses to HTML Accept

An authenticated administrator issued two read-only delivery-log requests with `Accept: text/html` on stock Original and ordinary Rust `e7dda7f`. Both Compose arms preserved the selected data and verified exact cleanup.

| Request | Original | Rust e7dda7f |
| --- | --- | --- |
| Formatted `.api` collection | 200, browsable HTML with the owned log | 404, JSON |
| Formatted `.json` collection | 406, JSON negotiation detail | 200, JSON with the owned log |

The successful diagnostic records two representation differences. The [ten previously matched JSON cases](rust-m0-e7dda7f-connect-log-json.md) cover their own requests and remain valid. The [machine-readable evidence](../scripts/rust-integration/m5-e7dda7f-connect-log-browsable.json) retains exact status, content type, response digest, and cleanup details for resolver ordinal 347.
