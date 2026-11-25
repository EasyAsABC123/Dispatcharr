# BE82 ordinary HLS and fMP4 workflows

The exact BE82 AMD64 package passed all four Docker fixture cells. They covered cold HLS reconnect and idle expiry, HLS restart after natural owner expiry, active-profile HLS, and two complete signed fMP4 deliveries with held-TERM coverage. The evaluator counted six complete media decodes.

The replacement token received the expected stale-playlist HTTP 404 before fetching the retained segment with HTTP 200. The retained segment matched all 492,936 saved bytes while the Redis index stayed at 2 and the segment TTL decreased. The same token then received a fresh HTTP 200 playlist and sequence 3 with the required discontinuity. Redis kept the same generation. The old and replacement workers opened two sequential Origin connections with at most one active connection at a time.

Physical execution, cleanup, independent evaluation, and the full engine inventory check each exited 0. The [typed proof](rust-be828d4c-hls-natural-restart-and-fmp4.json) preserves this result alongside the earlier comparator failure. The fixture establishes functional behavior. Broader M3 release gates and its paired performance measurements remain open.

Original Dispatcharr and all upstream contributors retain full authorship and license credit.
