# Seven required Compose workflows on a99e1772

Seven required workflows passed in one Compose wave against the verified
ordinary `a99e1772d1041d053c9e8716f662370dcff39bde` image. Each result
has a fresh qualification and an independent cleanup receipt.
[Machine-readable evidence](../scripts/rust-integration/m9-a99e1772-required-seven.json)

| Required node | Fresh scope |
| --- | --- |
| `package/clean-install` | Ordinary packaged clean install and startup |
| `package/live-react-stats` | Paired Original and Rust React playback and live Stats |
| `native/profile-delivery` | Native profile playback with qualified Original cache and media dependencies |
| `package/baseline-all` | Packaged baseline workflow |
| `package/current-packaged` | Packaged current workflow |
| `package/m4-functional` | Paired ordinary VOD and timeshift delivery |
| `package/profile-restart` | Paired Output Profile cache behavior across Redis restart |

The required 127-row matrix recorded seven passes, two retained Original
dependencies, and 118 blocked rows in this scoped wave. The coordinator
exited 2. The wave wrapper and independent reducer exited 0 and reported
exact cleanup. Full parity, soak, and application performance remain open.

Original Dispatcharr authors and contributors retain full application credit.
All dependencies retain their authorship and licenses. [Attribution](../ATTRIBUTION.md)
