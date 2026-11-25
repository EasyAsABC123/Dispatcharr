# ARM64 Original and relocated FFmpeg startup comparison

The ordinary ARM64 Rust application at `f38f6cdd09ad214d3d3ff18b0a46c286637fc9d1` and the pinned Original used the same FFmpeg executable bytes. Both version probes returned identical stdout within each fixture. Each ran before a media request, with no upstream connection, provider slot or registered client. The approved one-viewer fixture then decoded useful audio and video, drained its single upstream connection and encoder, and left the engine empty. Both completed packets and their independent audits exited zero.

| Fixture and launch order | Original file delta | Relocated file delta | Relocated minus Original | Original active-file delta | Relocated active-file delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| First fixture, Original then relocated | +61,235,200 B | +69,763,072 B | +8,527,872 B | +60,506,112 B | +68,841,472 B |
| Later fixture, relocated then Original | 0 B | −4,096 B | −4,096 B | 0 B | 0 B |

In the first fixture, each launch changed file charge in its own measured cgroup. The other application and Origin showed no file change at either startup. Anonymous bytes were flat. The relocated working-set delta was +69,128,192 B and the Original delta was +60,612,608 B. In the later fixture the relocated working-set delta was +692,224 B and the Original delta was +69,632 B. Before the first relocated launch, the candidate held 110,743,552 B in file charge. Before the later relocated launch, its fresh cgroup held 4,096 B. These are cgroup charges and do not give a VM-wide residency total.

Both launch paths produced substantial startup file charge in the first fixture, before playback. The difference between them did not reproduce in the later fixture. The two fresh projects ran in the same VM, so launch order and VM history are both different. The metadata observer obtained zero comparable file pairs out of the 56 fixed executable/dependency pairs in each fixture. Inode duplication, lower-layer backing identity and per-page charge ownership remain unverified.

The first formal S120 for this exact Rust binary exceeded the 64 MiB idle working-set growth limit at 90,247,168 B. That failure remains part of the record. This comparison carries no formal S120, S360, S7200 or release performance credit. A later S120 on the existing VM would be an observational repeat whose own unchanged formal gates determine whether it can supply a same-binary predecessor for S360.

Proof SHA-256 for Original first is `7fcc25a2d3b120827db2679fe2688b33f3593e74b641b21488c0ac4b578d269e`. Proof SHA-256 for the reverse order is `8adeabb55a1e3a48d983c722b5baf2bb23e9452a6256ce49f4566f011d98a0a2`. The exact FFmpeg SHA-256 is `7944790a81f0062178fb4c627450b413943542da226cb7bb2dd11db26421f5fb`. The initial packet's transfer omitted a helper and stopped at preflight with exit one and no workload. The corrected packets retained the exact donor helper bytes.

The original Dispatcharr authors and contributors and the upstream FFmpeg contributors retain full credit.
