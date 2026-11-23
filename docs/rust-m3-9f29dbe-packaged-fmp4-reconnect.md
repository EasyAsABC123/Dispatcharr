# fMP4 playback after abrupt owner loss on 9f29dbe

The ordinary AMD64 Rust image recovered fresh fMP4 playback on the surviving replica after the active owner was killed. The fixture used two copies of the same default package and a controlled H.264/AAC origin. Two attached viewers shared one upstream connection and one provider slot before the owner was killed. The exact owner exited with code 137 without an OOM, and its media processes retired. After the 75-second observation window, the surviving replica received a single authenticated request.

| Fresh request | Retained Original at `1241b3f7` | Rust `9f29dbe` |
| --- | ---: | ---: |
| HTTP status | 200 | 200 |
| Complete fMP4 fragments | 6 | 6 |
| Decoded H.264 frames with AAC | 74 | 74 |
| Additional upstream connections | 1 | 1 |

Rust returned `video/mp4` and the decoder consumed 1,733,540 bytes with exit 0. The fixture closed the live response after capturing enough media to decode. The new raw owner, a new transform bound to that raw owner in Redis, and a new remux process were independently observed. The predecessor fragment index was 8. The successor index was observed at 0 and advanced to 6. The origin served the same clip, and the six fragment hashes matched the older capture. The owner identities, process starts and index progression establish that the captured fragments were regenerated. Peak concurrent upstream connections and peak provider slots both stayed at one. The fixture recorded exact resource and process teardown, and the physical runner and reducer both exited 0.

The earlier [paired fresh reconnect](rust-e7-bd-reconnect-notification-connect-wire.md#fresh-fmp4-reconnect-after-abrupt-owner-loss) received 295,596 Rust bytes before a timeout, with no qualifying decode. The [longer Native diagnostic](rust-e7dda7f-clock-and-reconnect-generation.md#native-reconnect-generation-trace) then observed a new raw generation while the transformed profile and remux indexes stayed on the prior generation. The fix associates the profile owner's lease with its raw generation, fences mutations after that generation is lost, and lets a fresh viewer start new transform and remux generations after the old remux lease expires. Active legacy profiles without a usable binding retain their existing read path. Six Redis integration tests covered the lease and compatibility behavior, including legacy reads during renewal.

The package build, ordinary-image verification and source verification each exited 0. The tested application is `9f29dbedf8a144605dd05038024425d55b079ca0`; the image is `sha256:1cad036b0aa3d882d2d1ca201d7ea87def49be853f321421b71a6348e3a63936`. Publication verifies all 2,161 application inputs against the frozen package, including 497 protected React, license and attribution files. The [aggregate receipt](../scripts/rust-integration/m3-9f29dbe-reconnect-evidence.json) records the exact fixture and comparison identities.

This case qualifies a fresh request after the old remux lease expires. Same-socket continuation, abrupt loss of a legacy unbound profile, the rest of M3, and comparative performance remain separate acceptance requirements. The Original arm comes from the retained physical comparison, and only the Rust arm ran for this revision.

Original Dispatcharr and dependency contributors retain full credit.
