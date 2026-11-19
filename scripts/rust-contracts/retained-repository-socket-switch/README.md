# Retained Original repository, TS and handshake evidence

Original Dispatcharr authors retain full application and algorithm credit.

This packet joins 37 retained repository responses, three TS switch responses
and 17 WebSocket handshakes to existing resolver IDs. Six declarations remain
method-unresolved. Source24 records one final framework exception separately
from its 23 returned responses. The two Source2 responses actually returned 500
as configured in that separate APIClient capture. Actor labels describe the
fixture identities or supplied credentials. They do not establish a successful
authorization on an error response.

The historical WebSocket client snapshot is the exact executed credential-free
fixture tool recorded by the Source wire receipt. The current client adds
frame handling changes. Only the historical opening GET status is assigned.
Original TS change-stream targets use an owned channel placeholder.

To reproduce the binding with checked-in inputs, extract the preceding register
from `0bc11974d` and pass it to the packet generator.

```sh
git show 0bc11974d:docs/rust-contract-register.json > /tmp/dispatcharr-source264.json
python3 scripts/rust-contracts/retained-repository-socket-switch/reproduce.py \
  --register /tmp/dispatcharr-source264.json --output /tmp/dispatcharr-source270.json
cmp scripts/rust-contracts/retained-repository-socket-switch/bindings.json \
  /tmp/dispatcharr-source270.json
```

The reproduction starts no application and measures no performance.
