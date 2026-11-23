# ARM64 cold logical return on 0596767d

The exact Rust application `0596767d65b889424d529670cc7301b02564071b` completed a
disposable Linux ARM64 Docker Compose drill against Original
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The Native image was
`sha256:5d2856ca77a5b4e4d7b1725407e58e8f35659d14c984181f5244be9462327acd`.
The drill bound the verified ordinary package and signed-auth result for that image.
The owner, preflight, fixture, run and proof each exited 0. The receipt hash is
`914c80ca8c955dac93801b2b16ec4b0bd8a1b1c06910b53147340430bf5bae84`.
[Machine-readable evidence](../scripts/rust-integration/m10-0596767d-arm64-cold-copy-return.json)

The fixture used a synthetic common PostgreSQL logical archive to make two initial
restores. It then made two fresh cold restores. Its five preboot checkpoints
matched byte-for-byte across the scoped 62 tables and 501 columns, including row
ordering and sequence state. One ingress handled 14 requests, including 10 signed
requests. Eight requests used an Original-issued session. Native alone handled the
acknowledged rename. One row changed in `name` and `updated_at`. The Native
writer exited 0 and had zero SQL sessions after stop. Original then read the
acknowledged rename using that same session and issued zero reconciliation PATCHes.

The cold boots produced four bounded startup timestamp effects. Other checked
startup fields within each scope matched, and the post-ready read-to-stop state
matched. The independent audit read the retained host evidence without invoking
Docker. It verified that Compose down exited 0, the 16 owned containers, six named
volumes and one network were absent, and temporary Compose and archive files were
retired. The original project scope was empty. No raw data or credentials were
transferred by the audit.

The evidence covers a local same-schema logical return with a stopped writer.
Full M10 acceptance, mixed-schema rollback and production rollout remain open.
The drill recorded no performance measurements. The original Dispatcharr, React, PostgreSQL, Django,
Redis and dependency authors retain full credit.
