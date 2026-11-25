# B353 shared TS and HLS restart coverage

| Scoped row | Fresh physical outcome |
| --- | --- |
| `package/sharedTS18` | Passed three scenarios with six signed readers and six A/V decodes on the configured one-slot provider. |
| `package/sharedTS6-all-down` | Passed same-image recovery from zero readers after both workers stopped. Two signed A/V decodes and the one-slot provider check passed. |
| `package/retention100` | Passed 100 late readers across ten channels, with a 45-second history, ten decoded prefix captures, and no additional upstream. |
| `native/HLS-fMP4-workflows` | Cold HLS passed. The replacement Main became healthy and the signed restart flow timed out waiting for new media. Profiled HLS and fMP4 remained unrun. |

The group exited 1. The independent replay verified all three TS row results and their exact teardown. The HLS run’s last predecessor sample recorded sequence 2 with 43.782 seconds of remaining lifetime. The first sample after the fresh signed request arrived within five seconds of the earlier sample. Forty post-request observations showed no owner, index zero, and no retained segment. The point at which the keys disappeared remains under investigation. [Typed evidence](rust-m3-b3530c8e-shared-ts-and-hls-restart.json)

The independent teardown checked all four exact container identities and recorded host process generations, empty owned container, network, and volume inventories, and a settled process group. It archived the temporary cleanup hold under the dispatcher lease. The original workflow and callback exits remain failed. Full M3 and paired performance remain open. Original Dispatcharr and all upstream dependency authors retain full credit.
