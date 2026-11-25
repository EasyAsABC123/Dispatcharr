# D6 ARM64 VOD reconnect role attribution

The exact retained D6 R5 pair used **+126,562 µs (+5.84%)** more CPU and
**38.37% less sampled peak concurrent working memory** than Original.
It completed matching signed Origin and decoder work. The read-only role
projection verified the matched proof SHA-256
`fdc8dc687be9bf80304408b06efedb753ded84a9da731c5f39ddb1279279bb8e`
and reconciled all five role deltas in integer microseconds.

| Role | Original CPU, µs | Native CPU, µs | Native − Original, µs |
| --- | ---: | ---: | ---: |
| Backend | 226,385 | 212,808 | −13,577 |
| PostgreSQL | 1,088,735 | 1,175,233 | +86,498 |
| Redis | 162,203 | 230,872 | +68,669 |
| Origin | 39,868 | 32,517 | −7,351 |
| Signed probe and decoder | 648,505 | 640,828 | −7,677 |
| **Five-role total** | **2,165,696** | **2,292,258** | **+126,562** |

PostgreSQL and Redis together accounted for +155,167 µs, offset by −28,605 µs
across the other roles. This locates the charge in this single pair.
It does not distinguish additional statements or Redis commands from cost
per operation, background work, or scheduling. The independent observer
is outside the five-role sum. There is no historical per-role comparison
or f38 physical measurement in this projection. [Typed evidence](rust-m4-d6-arm64-vod-role-attribution.json)

Original Dispatcharr and all its upstream authors and contributors retain
full application, design, implementation, documentation and test credit.
FFmpeg, PostgreSQL, Redis and dependencies retain their authorship and licenses.
