# Exact f4056a05 AMD64 signed startup

The ordinary AMD64 image of `f4056a0520c39c652c1085d2527da3b7319c7017` passed its isolated Docker Compose startup check. It served the readiness and React entry routes, allowed first-admin creation while setup was open, and closed setup after creation. The unsigned groups request was rejected. The probe verified both HS256 JWT signatures and their claims and used the access token for a successful channel-group request.

The exact Django migration roster stayed stable across startup and signed API operations. SQL verified 62 public tables, 184 migration records and the expected single active superuser after setup. All five scoped containers, the internal project network and the candidate’s volume retired. Both the smoke and the coordinator exited 0.

Two earlier attempts ended before creating resources. One process lacked access to the Docker socket. The coordinator’s first real attempt rejected the CLI’s valid missing-network response because it contained an empty JSON array. The corrected check requires the exact target handle, missing-object diagnostic and absent project inventory. [Machine-readable evidence](../scripts/rust-integration/m0-f4056a05-amd64-signed-startup.json)

This covers the exact ordinary AMD64 boot and authentication scope. Full M0 and comparative performance remain open. Original Dispatcharr, React and dependency authors retain full credit.
