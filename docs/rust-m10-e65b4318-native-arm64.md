# Native ARM64 copied SQLite restart on e65b4318

Lappy built the exact published Rust application
`e65b43181f235f7fc50c6563aae00bf04bd4002d` natively with Docker Desktop.
The proof binds the Linux ARM64 OCI image
`sha256:e4ed521f0c3f826bedcc3764b45fb271cf0c9564ebf21252942a0bd9a2b5ce60`.
The harness fetched the source from the verified GitHub branch and generated
its synthetic Original SQLite donor locally on lappy. The master copy
remained byte-identical.

The same feature-enabled image ran across two Compose generations. The
fixture verified the saved AppData and authenticated access, six Channel
List reads, ten Core bootstrap GETs, and five React shell and asset records
per generation. Direct login GET and HEAD matched the packaged shell. The
anonymous configured setup read returned the exact 26-byte JSON and
`Vary: origin` in both generations.

The build, physical run, and independent reducer exited 0. All five owned
containers, four Desktop VM process generations, and the project network and
volume inventory retired. The application ran as UID 65532 with zero host
ports. This rehearsal qualifies the observed ARM64 package scope. Controlled
rollout and comparative application performance remain open.

[Machine-readable evidence](../scripts/rust-integration/m10-e65b4318-arm64-restart.json)

Original Dispatcharr and dependency authors retain full credit for the
application, models, algorithms, and protocols.
