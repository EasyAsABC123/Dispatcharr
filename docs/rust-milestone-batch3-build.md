# Combined runtime-fix Docker candidate

Committed application `bfb4086fe6d65a9db5151e95b478f76b2094bc90` builds as:

| Identity | Value |
| --- | --- |
| Image | `sha256:29f68ba847b3b8e083e9e77e28a2a8446273a420a0c78887c6dba1bffcacbee9` |
| Source tree | `01726c2090117d8f817415721d65bd970bb640b288b2772f5da2c804fd4fa767` |
| Production ELF | `21696a609013f1777127b667498def257ac6068d5cd8c524390c70d184344934` |

The [actual build and probes](../scripts/rust-integration/milestone-batch3-build/Root-build-review.json)
pass. Every1,421 archived input matches Git:924 Rust-directory files,495 unchanged
React files and both original credit/license files. The runtime closure matches
the [888-test candidate](rust-milestone-batch3.md). UID65532, actual executable
entrypoint and114 installed frontend/credit assets are verified.

Packaged GPG2.4.8 runs and all27 crypto/trust/owning-package copyright hashes
match. Exact probe-container absence is verified. Original Dispatcharr and
dependency authors retain full application, algorithm and license credit.

```sh
python3 scripts/rust-integration/build.py \
  --git-revision bfb4086fe6d65a9db5151e95b478f76b2094bc90 \
  --tag dispatcharr-rust-candidate:milestone-batch3 \
  --output /absolute/path/to/fresh-build --timeout 1800
```

One image supplies the affected packaged job, cache-timing, signature and React
cohorts. Build success alone does not qualify those runtime behaviors, full
milestones or resource gains. The earlier resource preflight failed its frozen
useful-work gates and supplies no accepted performance comparison.
