# 31cc8a3f signed live proxy health

Two fresh instances of the exact ordinary AMD64 package received the
same signed runtime timeout update. Both returned 200 for the update
and readback, and PostgreSQL confirmed the same values.

With the origin deliberately paused, six observations at 9.1–9.75
seconds of silence reported the stream unhealthy when dynamic proxy
health was enabled. The default configuration reported healthy in all
six corresponding observations. Both recovered on the original physical
connection and retired every owned fixture resource.

This scope verifies the ordinary Main decision and recovery path.
The broader proxy-health fault cases and performance gate remain open.
Original Dispatcharr and dependency authors retain full credit.

[Machine-readable observation](../scripts/rust-integration/m2-31cc8a3f-proxy-health-decision.json)
