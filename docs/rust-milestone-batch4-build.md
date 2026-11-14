# Shared settings and fresh-install Docker candidate

Original Dispatcharr and dependency authors retain all application, algorithm
and license credit. The unchanged React assets retain their original credits.

Committed runtime `9440a31973978e6c8efe144912525c59fa3f260f` builds as image
`sha256:a6d563ff2f71d9552df2e3d28d10e949b99a891d880305b484416e73733fa4f6`. The source tree is `a1ab5a80e210881fa90fc55eebfb0128ae9560f1023bf3c82e42c9670d044859`
and the production ELF is `34125de3436ef169741874d3962d3222ad3e828f2c63ceab5f668c6713382350`.

[The build and physical probes](../scripts/rust-integration/milestone-batch4-build/Root-build-review.json)
pass: all1,421 archived files match Git, the Rust closure matches the
[890-test SDK candidate](rust-milestone-batch4.md), UID65532 and the real ELF
entrypoint are verified, and all114 installed React/credit hashes are unchanged.
The packaged GPG/trust/copyright closure is independently checked. Probe
containers are absent afterward. One image supplies the affected full job,
settings/plugin and clean-install cohorts; build success does not establish
their acceptance or a performance gain.

```sh
python3 scripts/rust-integration/build.py \
  --git-revision 9440a31973978e6c8efe144912525c59fa3f260f \
  --tag dispatcharr-rust-candidate:milestone-batch4 \
  --output /absolute/path/to/fresh-build --timeout 1800
```
