# Whole-milestone candidate Docker build

The committed `1dc16f8da8b04a0b119afebef16649b837c6e928` application builds as:

| Identity | Value |
| --- | --- |
| Image | `sha256:e88cc73fb054d6b285a99018e70b60b072ea0b32d96408374e2109e6692ece65` |
| Source tree | `c039869f1b521ab7518731dc49e0bcd277679f373c02d0aa5a53fadff8f07a39` |
| Production ELF | `43b0ea49ac2d68322fa7f9b0549870be76a106b67b591d250a5d43bb402e2e91` |

The [actual build](../scripts/rust-integration/milestone-batch2-build/build.json)
and installed-file probes pass. Every1,416 archive file matches Git:919 Rust,
495 unchanged React and the two original credit/license files. The Rust closure
matches the [882-test validated candidate](rust-milestone-batch2.md). The image
runs as UID65532 with the real production entrypoint;114 frontend/credit assets
remain byte-identical to image57.

[Packaged crypto checks](../scripts/rust-integration/milestone-batch2-build/Root-crypto-runtime-probe.json)
execute the GPG wrapper successfully (`GnuPG2.4.8`) and verify27 binary, library,
trust and copyright files. Every ELF dependency has its owning-package notice
retained, with byte-identical reuse of existing notices where applicable. Original
Dispatcharr and dependency authors retain full credit and licenses. Probe
containers retire with exact absence checks.

```sh
python3 scripts/rust-integration/build.py \
  --git-revision 1dc16f8da8b04a0b119afebef16649b837c6e928 \
  --tag dispatcharr-rust-candidate:milestone-batch2 \
  --output /absolute/path/to/fresh-build --timeout 1800
```

One shared image is used for the prepared plugin41, job and input-profile
functional cohorts. Build/version checks alone do not establish signature,
worker, media, shutdown or performance acceptance. Those actual results remain
separate; no historical resource gain transfers to this candidate.
