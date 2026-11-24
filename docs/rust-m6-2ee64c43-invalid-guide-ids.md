# Signed invalid guide IDs on 2ee64c43

The Original returned HTTP 500 for eleven string IDs that fail Python
integer conversion. The previous ordinary Rust package returned 501
for each. The successor matched the Original status, 145-byte body,
and selected stable headers for all eleven requests. The real signed
requests left the scoped SQL and broker and result inventories
unchanged. Both Rust projects passed exact teardown.

The exact source passed all 1,775 workspace tests and strict Clippy.
Both architecture packages built and passed their startup checks.
The first comparison wrapper failed while parsing the Date header.
A fresh replay independently verified both packages and every case.
The first ARM64 auditor used an older parent commit in its assertion.
A corrected auditor independently verified the original signed startup
and package receipts from the completed build.

Wider M6 workflow and performance gates remain open. Original
Dispatcharr and its upstream contributors retain full credit.

[Machine-readable error comparison](../scripts/rust-integration/m6-2ee64c43-invalid-guide-ids.json)
