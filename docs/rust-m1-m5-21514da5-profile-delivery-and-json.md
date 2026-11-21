# Configured stream delivery and Channel Profile JSON on 21514da5

The Rust MPEG-TS configured-profile response holds one delivery slot until the final owner of its bytes releases it. A peer on the same pipeline has a separate slot. Focused tests cover cloned bytes and viewer or source stop while a frame remains held.

In the exact paired Compose test, Original and Rust each served a raw anchor and two configured MPEG-TS viewers through one upstream connection and one encoding process. The slower client read 16 KiB every 110 ms. The other client continued receiving media after it closed. Both profiled clients decoded audio and video on each backend. Exact owned processes and Compose roles retired. The first reducer used an outdated cleanup field name and exited 1. Its successor read the independent cleanup receipts and verified all five container IDs and their process generations on the same physical run.

Twenty direct JSON Channel Profile reads per backend matched the pinned renderer and typed records for Administrator and assigned Standard users. Four compact controls per backend and unchanged owned PostgreSQL rows verified that the aliases still share the normal read paths. The tested cases cover indentation 2, 3, and 4, quoted indentation, and the clamp to 8. Broader media-range and browsable HTML handling remain open.

The exact application source passed formatting, strict Clippy, and 1,494 enabled Cargo tests. The paired scopes leave integrated parity and comparative performance open. [Exact revision and receipts](../scripts/rust-integration/m1-m5-21514da5-profile-delivery-and-json.json)

Original Dispatcharr, FFmpeg, Django REST Framework, and dependency authors and contributors retain full credit. [Attribution](../ATTRIBUTION.md)
