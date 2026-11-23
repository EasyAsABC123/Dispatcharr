# M2 shared live TS with 100 signed viewers on AMD64

Original Dispatcharr and Rust each served 100 concurrent authenticated
connections to one live TS channel on Beepboop. Each arm opened one
connection to the synthetic upstream and used one provider slot. Both
stayed at one active upstream throughout the measured overlap while the
origin supplied more bytes. The provider’s cumulative connection counter
advanced from 0 to 1 across Original and from 1 to 2 across Rust.

| Physical check | Original | Rust |
| --- | ---: | ---: |
| Continuous HTTP 200 connections with advancing media | 100 | 100 |
| Distinct client registrations under the seeded identity | 100 | 100 |
| Independent completed FFmpeg audio and video decodes | 100 | 100 |
| Video frames in each verified capture | 72–98 | 124 |
| New upstream connections across the complete arm | 1 | 1 |
| Active upstream connections at both overlap boundaries | 1 | 1 |
| Assigned provider slots at both overlap boundaries | 1 | 1 |

The isolated Compose fixture used the same synthetic provider, one seeded
channel and one disposable identity. It checked all 100 independent
sessions against that identity in Redis. After a startup prime, each arm had
five seconds of warmup and five seconds of overlap. Every viewer made one
request, delivered bytes during the overlap and reached the final barrier
without an unexpected end of stream. The audit verified the SHA-256 of all
200 retained 2 MiB captures and all 200 decoder transcripts. FFmpeg completed
with both video and audio mapped for every capture and at least 25 video frames.

Each arm retired its clients and provider assignment and returned the origin
to zero active connections. Compose removed all five owned container IDs,
including the completed initialization container, and retired the four recorded
live process generations. The owned container, network and volume inventories
were empty. The coordinator, preflight, physical run and reducer exited 0.

The Rust application was `8a547cf00cd33a94fe4ccb982272be1861d52e73`. The
ordinary Linux AMD64 build manifest was
`5aac266c185bbaa121a1b890bbeb7455e5b606c30f04c5b966a2cebc8b4e48c8`.
The stock Original revision was `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.
The evidence covers shared single-channel TS on AMD64. Comparative CPU and
memory measurements and formal M2 acceptance remain separate gates.
[Scoped evidence](../scripts/rust-live-perf/m2-8a547cf0-amd64-shared-ts-100.json)

The original Dispatcharr authors and contributors retain full application
and protocol credit. Existing React, FFmpeg and dependency credits are preserved.
