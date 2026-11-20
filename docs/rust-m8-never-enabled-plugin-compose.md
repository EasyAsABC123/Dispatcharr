# Never-enabled plugin listing and settings in Compose

Original Dispatcharr authors and contributors retain all application credit.

Pinned Original and the exact `8b0c76ca` Rust package returned HTTP 200 for
the same four requests in an internal Docker Compose project. Standard and
administrator lists matched, an administrator saved settings, and a standard
read verified that both the known and unknown setting keys persisted. Each
response matched on decoded JSON and the checked Content-Type, Allow and Vary
headers. Each backend's Content-Length matched its own response size.

Original registered the local manifest through its pinned plugin-discovery
entry point. The fixture retired that registry row before Native started.
Stock Rust Main then independently registered the same manifest. Both
backends left the plugin never enabled, and neither created its import marker.
The comparison exited 0 and all four owned generations retired with no
project resources left.

The first two attempts exited 1 before Original's first plugin request because
the fixture registry check failed. The second attempt recorded zero registry
rows after a successful targeted migration. The third attempt reached both
applications and matched the decoded responses, valid lengths and settings
effects. It exited 1 because all four c74 responses sent `Vary: origin`
while Original sent `Vary: Accept, origin`. The successor declares the Accept
variation for plugin listing and settings responses. The measured responses
then matched on `Vary: Accept, origin` in all four cases.

The exact successor also passed the focused response test, all 13 existing
plugin-action tests and strict Clippy. This evidence covers the never-enabled
manifest and settings workflow. Enabled-plugin execution, broader M8
acceptance and performance remain open.

[Redacted scoped receipt](../scripts/rust-integration/milestone-batch113/never-enabled-plugin-vary.json)
