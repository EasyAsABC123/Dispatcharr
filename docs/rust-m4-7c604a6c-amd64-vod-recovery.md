# M4 VOD recovery on AMD64

These two independent physical comparisons used the ordinary Linux AMD64
artifact built from `7c604a6c77fb791c8d4b3b1e30fc58148875f04e`, recorded
at `49f6c5faf9da691d01b43f776ce0fd14da4c73b0`, and pinned Original
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The tested manifest SHA-256 is
`7ea5c3e1b3ab30513b071acc2b9640b5e49bc660de743f0d2ea569c170d685fc`.
This evidence is prepared for publication snapshot
`b01ebff604379ad75198f2c8780cfc8505ecf964`. Its runtime and ARM64 artifacts
remain outside both comparisons.

## Upstream 503 and fresh authorized retry

Both arms observed upstream 503 then 200 and downstream 500 then 200. The
fresh authorized retry returned the exact 4,299,455-byte synthetic MP4 with
successful audio and video decoding. Each attempt held one provider slot
and one active reader while in flight. Both sessions retired, with the
slot released after each attempt. All four owned container generations
were absent after teardown and the project inventory was empty. The cold,
physical, reducer and campaign exits were all zero.

**Exact error-body parity failed.** Original returned a 116-byte upstream
status explanation with the URL redacted from this publication. Rust
returned a 44-byte generic provider failure. The sanitized receipt records
HTTP 500, body length, SHA-256 and classification for each arm. The zero
reducer exit qualifies recovery and teardown while
`one_to_one_parity_passed` remains false. Normal provider URLs can carry
credentials. The exact body remains an open compatibility difference and
the generic Rust error preserves the disclosure boundary.

## Active VOD across one owned Redis replacement

The bounded gate passed. With one signed viewer capped at one stream,
both backends kept the first HTTP 200 response open through replacement
of the isolated Redis process with empty state and delivered a matching
278,528-byte prefix. The same authorized playback ID reopened with HTTP
200 and an identical 8,192-byte prefix. The old response continued with
8,192 matching bytes at offset 278,528. The candidate process identity
stayed the same. Physical capture, independent empty project inventories
and strict reduction all exited zero.

The image34 failure and the later `0c5133b62` scoped pass retain their
historical artifact identities. The current Redis-loss receipt covers one
replacement and one active same-ID reopen. Full-file decode for that case,
event details, broader post-reopen close or release behavior, repeated
loss, comparative performance and full M4 remain open.

[Machine-readable evidence](../scripts/rust-integration/m4-7c604a6c-amd64-vod-recovery.json)

Full credit for Dispatcharr's original application, design, features, user
interface, implementation, documentation and test suite belongs to the
original Dispatcharr authors and contributors, including OkinawaBoss,
dekzter, SergeantPanda, Seth and the upstream contributor community.
FFmpeg and dependency contributors retain their own credit.
