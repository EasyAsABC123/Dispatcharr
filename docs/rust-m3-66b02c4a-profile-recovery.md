# Native FFmpeg profile recovery on 66b02c4a

The ordinary `66b02c4a` image recovered an owned FFmpeg output profile
during a local Compose run. The unprofiled viewer kept receiving media
and the raw stream kept its Redis owner and one upstream connection. A
fresh authenticated profile request produced a new FFmpeg child on the
same Main process. A/V decoded before and after the replacement.

The fixture verified both child generations and their pipe descriptors.
Both children and their profile state retired. All three owned Compose
containers and their process generations retired.

The first attempt stopped before the recovery workload because an
inherited Original-only step expected a web role in the Native fixture.
The corrected observer reused the exact ordinary image. The first exit
and verified cleanup remain recorded.

The full Native profile-delivery row and comparative performance remain
open. [Machine-readable evidence](../scripts/rust-integration/m3-66b02c4a-profile-recovery.json)

Original Dispatcharr, FFmpeg, and dependency authors retain full credit
for the application, models, algorithms, and protocols.
