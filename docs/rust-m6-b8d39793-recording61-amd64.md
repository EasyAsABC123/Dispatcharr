# M6 natural 61-second recording on b8 AMD64

Pinned Original and the ordinary Rust AIO package each accepted a signed recording booking 60 seconds before its 61-second window. Both returned the recording in progress and then completed, with HTTP statuses 201, 200 and 200. Each arm observed one natural capture from the same paced synthetic origin and produced a 46.330-second Matroska file. The origin was capped at 45 seconds per connection.

| Physical observation | Original | Native |
| --- | ---: | ---: |
| First recorder observation after scheduled start, upper bound | 0.484 s | 0.243 s |
| Recording task publications on Redis DB1 | 1 | 1 |
| Same-value TTL replenishments of the active marker | 3 | 3 |
| MPEG-2 Main video frames | 1,155 | 1,155 |
| MP2 mono 48 kHz audio frames | 1,920 | 1,920 |
| Declared whole-file audio/video decode exit | 0 | 0 |
| Separate strict `-xerror` decoder exit | 183 | 183 |
| Owned containers and original host generations retired | 2 | 3 |

The signed route's scheduler name joined to one real execution UUIDv4 per arm. Its DB1 envelope matched the four booking arguments and the Redis DB2 lookup for the same UUID returned Celery `SUCCESS` with a null result. The audit independently reconstructed the DB2 command digest and checked the retained response. Both runs observed one recorder, provider slot and upstream connection before those resources and the active cache marker retired. The default Recording SQL was unchanged. All five owner phases exited zero, and the scoped Compose container, network and volume inventories were empty. The declared decode reached the final progress marker and both output EOFs. The separate strict results preserve the known decoder diagnostics.

The Rust application revision was `b8d3979366981f0372794f8e04b01763a233bae7`, Linux AMD64 image `sha256:25414ab4350b0eb74f0e7c8efb0343676a88961bae38880bc758181c0496d155`, with build manifest SHA-256 `654a678fbb369613d9e2cac184035969ad46c05b20440656cca818ee757a96f2`. Original was `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. Original Dispatcharr authors and contributors retain full application and React UI credit. Django, Celery and FFmpeg retain their authorship and licenses.

The receipt-only resolver emitted the Recording61 plan and verified the evidence without creating runtime resources. The exact candidate identity matches the existing b8 XMLTV pair that verified Guide Current and title replacement. Formal acceptance still requires the coordinator's one-off Beat prerequisite, approved adapter registration and same-invocation package evidence for the guide nodes. Full M6 and performance qualification remain open. [Matching XMLTV pair](rust-m6-b8d39793-ordinary-complete-refresh.md) · [Sanitized physical evidence](../scripts/rust-milestone-batches/m6/verified-b8d39793-recording61-amd64.json)
