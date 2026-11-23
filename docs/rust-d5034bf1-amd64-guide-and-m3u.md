# d5034bf1 AMD64 guide and provider routes

The ordinary AMD64 image built and verified from application
`d5034bf1f335f2715456bf38779d36f0f8aea256`. The host workspace passed 1,759
tests, with 822 ignored and zero failures. Formatting and strict Clippy passed.
The existing React frontend and Dispatcharr attribution and license files
retain their original contents and modes. The vendored assets carry their
upstream notices. Original Dispatcharr, Django, React and other dependency
authors retain full credit.

Two signed Native EPG import requests used the retained matching Original
responses for list and object source IDs. Both returned HTTP 500 and the
same 145-byte HTML entity. The broker, result store and scoped database
checks passed, and the fixture retired its owned resources.

The provider-management comparison observed 72 signed or anonymous role
cases across 19 routes and methods. Eight matched. The other 64 returned
404 across 16 missing JSON-suffix route methods. The canonical profile PATCH
and the remaining server-group read-role cases matched. Execution and exact
cleanup passed. The provider parity assertion failed.

[Machine-readable qualification](../scripts/rust-integration/d5034bf1-amd64-guide-and-m3u.json)
