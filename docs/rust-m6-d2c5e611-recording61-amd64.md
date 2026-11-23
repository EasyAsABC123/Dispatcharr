# M6 natural 61-second recording on AMD64

Stock Dispatcharr and the ordinary Rust AIO package each accepted a signed booking 60 seconds ahead of a 61-second recording window, returned the active recording and then returned its completed state. Both produced a 46.330-second Matroska recording from the same paced synthetic origin, which was capped at 45 seconds per connection. The signed HTTP statuses were 201, 200 and 200 in each arm. The recording reached completion without an operator stop or an injected task.

The two isolated Compose projects used Original revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and Native revision `d2c5e6113efe947551c17bf877e8c6b2c964190c`. The Native Linux AMD64 image ID was `sha256:212432b13a1e0d8aa5aaf1ece7c34748c427d06e6a094cd385529e43f33527d6`. The exact build manifest SHA-256 was `01103d3bb13f2e3fddd934a9ad287e04e87751e5c4112825f22c5d931835e650`. The original Dispatcharr authors and contributors retain full credit for the application and React UI. Django, Celery and FFmpeg retain their respective authorship and licenses.

| Physical observation | Original | Native |
| --- | ---: | ---: |
| First recorder observation after the scheduled start, upper bound | 0.716 s | 0.215 s |
| Recording task publications on Redis DB1 | 1 | 1 |
| Same-value active marker TTL replenishments | 3 | 3 |
| MKV duration | 46.330 s | 46.330 s |
| MPEG-2 Main video frames | 1,155 | 1,155 |
| MP2 mono 48 kHz audio frames | 1,920 | 1,920 |
| Declared whole-file mapped decode exit | 0 | 0 |
| Separate strict `-xerror` decoder exit | 183 | 183 |
| Owned containers and original host process generations retired | 2 | 3 |

The scheduler stored one enabled one-off row with the same four arguments as the signed booking. Its route-visible scheduler name joined to one real UUIDv4 publication in the `dvr` queue. The publication's root, correlation ID, empty parent and scheduled interval matched the booking. The audit read the retained broker journal and reconstructed the separate Redis DB2 lookup command hash. Both actual DB2 responses recorded the same execution UUID with Celery `SUCCESS` and a null result. Each arm observed one recorder, provider slot and upstream connection at capture, then observed the owned recorder generation, HLS working directory and active cache marker disappear. The copied fixture's default Recording SQL stayed unchanged and the DVR queue and reservations drained.

Both declared decodes mapped the complete video and audio streams and reached `progress=end` and both output EOFs with the decoder process group retired. Both reported the two already-known MPEG-2 diagnostics. The strict decoder results preserve their nonzero exit and stderr hashes separately. The two files have the same size and decoded frame counts, with distinct file hashes.

All owner phases exited zero, including preflight, execution, cleanup, proof and the outer campaign. The four Original application roles exited cleanly. Native exited zero without an OOM. The retained cleanup joins confirmed every scoped container ID and host process generation absent and the labeled Compose container, network and volume inventories empty.

The receipt-only adapter emitted a dispatch plan for this exact package. Formal acceptance still requires the coordinator to admit it through the canonical M6 resolver with the required one-off predecessor and the same-package guide and second-import sequence. This pair is scoped functional evidence. Full M6 and performance qualification remain open. [Sanitized physical receipt](../scripts/rust-milestone-batches/m6/verified-d2c5e611-recording61-amd64.json)
