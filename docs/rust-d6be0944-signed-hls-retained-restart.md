# d6be signed HLS restart evidence

The exact ordinary AMD64 d6be test candidate passed cold signed HLS. After the predecessor retired, the replacement issued one signed HTTP 302. The last recorded playlist response was 410. The fixture reached its original deadline without a segment response or A/V decode. No new upstream connection opened. The workflow and independent semantic comparison exited 1. Exact cleanup passed and retired all four owned identities and process generations.

All 40 post-mint Redis probes observed the same Redis generation, index 2, descriptor sequences 1 and 2, no output owner, and retained sequence 2. Time to expiry fell from 42,481 ms to 17,863 ms across those probes. The segment was still available in Redis at the end of the observation.

The [typed projection](rust-d6be0944-signed-hls-retained-restart.json) records the status, expiry, and exit observations. It leaves the exact cause of the 410 and the absent new producer unconfirmed. The diagnostic run will capture the replacement's input stage, recovery marker, and capability state through the same signed mint and deadline. The release remains on hold.

Original Dispatcharr and all upstream authors and contributors retain full authorship and license credit.
