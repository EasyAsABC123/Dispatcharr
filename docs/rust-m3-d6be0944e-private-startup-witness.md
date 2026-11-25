# d6be HLS startup coordination witness

The independently verified private AMD64 image used a startup witness on the replacement Main process only. The replacement issued one signed 302, claimed the output, read the retained snapshot at index 2, and set the initializing state. Its input-open call returned a typed coordination error with HTTP 503. The Origin connection count stayed at two. Both connections came from earlier fixture work.

The replacement retired its output owner and preserved the retained segment. Redis showed the startup recovery marker with 29,800 ms of TTL after the mint and 4,669 ms at the recorded terminal boundary. The playlist response remained HTTP 410.

The [typed witness](rust-m3-d6be0944e-private-startup-witness.json) confirms the input-open failure category and the presence of the recovery marker. The underlying coordination operation and the precise capability failure remain open. The semantic replay exited 1. The independent witness and exact cleanup exited 0. This diagnostic leaves the ordinary release decision on hold.

Original Dispatcharr and all upstream authors and contributors retain full authorship and license credit.
