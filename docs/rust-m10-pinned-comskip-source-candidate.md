# Commit-pinned Comskip build candidate

A possible release packaging path is to build Comskip from an explicit
upstream source revision. The candidate archive pins revision
`a140b6ac8bc8f596729e9052819affc779c3b377`. Verification reproduced its Git
tree and checked every one of the 42 file blobs, executable modes, and the
archive SHA-256. The build applied the FFmpeg compatibility edit from the
original Dispatcharr recipe.
[Upstream revision](https://github.com/erikkaashoek/Comskip/commit/a140b6ac8bc8f596729e9052819affc779c3b377)
[Original recipe](https://github.com/Dispatcharr/Dispatcharr/blob/1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430/docker/DispatcharrBase)

The source compiled from a fresh copy on the Linux AMD64 host with GCC 16.2.1
and the host FFmpeg libraries. The two private argtable2 packages matched the
Debian signed index. The detector reported version 0.83.001, processed eight
seconds of generated MPEG-TS, and returned its no-commercials status with an
empty EDL and the input hash preserved.

Three setup or validation issues were corrected. The host lacked
`dpkg-deb`, the first configure invocation did not resolve the privately
extracted argtable2, and the first successful compile's help check rejected
Comskip's observed help exit 2. The fresh final host controller exited 0.

This is an alternate candidate for a future package. Establishing exact
corresponding source for the existing Original binary remains a separate gate.
ARM64 execution, packaged dependency notices, native media behavior, and
distribution qualification remain open.
[Redacted host build receipt](../scripts/rust-integration/m10-pinned-comskip-source-candidate.json)

The original Dispatcharr authors, Erik Kaashoek and Comskip contributors,
and the dependency authors retain full credit.
