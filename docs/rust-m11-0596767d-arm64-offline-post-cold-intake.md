# ARM64 offline release intake on 0596767d

The local M11 intake admitted the exact ordinary ARM64 package, its signed-auth
receipt and the independently audited same-schema cold return for
`0596767d65b889424d529670cc7301b02564071b` for scoped review. The intake and its
replay exited 0 and returned byte-identical results. The receipt hash is
`d13d15c0769e3eb037bb95942410e81f656887f78342e948e4c02df8c0dd7bbc`.
[Machine-readable decision](../scripts/rust-integration/m11-0596767d-arm64-offline-post-cold-intake.json)
[Cold-return evidence](rust-m10-0596767d-arm64-cold-copy-return.md)

All 20 denial controls passed. They cover older or mixed candidates, altered
package and signed-auth evidence, incorrect configuration and restored-state
digests, incomplete teardown, self-asserted authority and submission of a local
cold return as full M10 acceptance. The intake denied the switch and issued zero
deployment actions or fallback components. It exercised a local policy replay
with zero engine and network calls.

Release qualification still requires formal M9 acceptance, full M10 operator
handoff and rollout proof, approved local retention, same-configuration
compatibility, operator signoff and distinct release authorization. This intake
gives current-package credit to its signed-auth receipt. Current signed-browser
qualification is open in this intake. The original Dispatcharr and dependency
authors retain full credit.
