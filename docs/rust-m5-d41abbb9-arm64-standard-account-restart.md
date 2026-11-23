# Signed Standard-account session through an ARM64 restart

Fresh Chromium sessions on Lappy compared Original
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` with the exact ordinary ARM64 Rust
package for `d41abbb90dec665182f2060804e40a4d522b1743`. Both application arms,
both browsers, the physical owner and the independent audit exited 0. Both arms
served the same 112-file React asset manifest.
[Machine-readable evidence](../scripts/rust-integration/m5-d41abbb9-arm64-standard-account-restart.json)

In each arm, an administrator provisioned one synthetic Standard user. The
Standard user signed in and opened Channels. Chromium kept the original access
token in memory while the controller stopped only the application. Both old
processes exited 0. The listener was closed between generations, and the same
container and image served again from a new process generation. PostgreSQL,
Redis and the browser origin retained their process generations. The controller
verified identical ordered SQL account rows and the same Django session count
before and after restart.

The same browser context sent the same signed bearer after restart and received
HTTP 200 with an unchanged typed Standard profile. The Channels view remained
visible. The user made one signed logout, which returned HTTP 200. The independent
audit checked the recorded stop, wait, listener probe, start and readiness
command sequence. Both temporary browser profiles retired, and both Compose
down commands exited 0. Exact typed absence covered seven Original containers,
six Rust containers, four networks and two volumes.

This verifies a planned application process restart in a disposable same-host
ARM64 fixture. Container replacement, restarting PostgreSQL or Redis, and broader
settings changes remain outside this result. Full M5 acceptance remains open.
The original Dispatcharr authors and contributors retain full application and
React frontend credit.
