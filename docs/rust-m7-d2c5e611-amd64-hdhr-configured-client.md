# M7 AMD64 configured HDHomeRun client

Pinned Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and packaged Native `d2c5e6113efe947551c17bf877e8c6b2c964190c` matched in a fresh configured-client replay on one internal Docker Compose network with no published ports. The exact Native package manifest is `01103d3bb13f2e3fddd934a9ad287e04e87751e5c4112825f22c5d931835e650`. Preflight, the paired fixture, proof and both owner exits were zero.

In each arm the client fetched `/hdhr/discover.json`, followed its advertised lineup URL, and opened the generated channel URL. Discovery and lineup returned HTTP 200, `application/json`, the same selected `Allow` and `Vary` headers, and matching projected digests. The stream returned HTTP 200 as `video/mp2t`. Both arms returned the same 1,316,000-byte excerpt of 7,000 aligned MPEG-TS packets by digest. The bounded strict decoder exited zero with empty stderr, decoding H.264 video and AAC audio and reporting 74 video frames per arm. Each arm opened one synthetic primary upstream connection. It retired after the client closed, and the fallback origin saw no change.

The fixture retired the Original web process and cleared its owned Redis state before switching to Native. Native exited zero without OOM. Teardown checked the exact owned container IDs and original process generations for baseline, origin, data-init and candidate. It also verified empty container, network and volume project inventories.

The pinned Original archive defines HTTP discovery routes and no configured HDHomeRun UDP responder was found in its startup or deployment scope. The relevant Native HTTP entry points match the prior `60c7637c` assembly byte for byte. Its only UDP socket identified in the reviewed source is used for IP lookup. An automatic discovery test requires a separate configured responder on the same owned network. This run sent no UDP discovery traffic and establishes no multicast, hardware-client, performance or full M7 result.

[Machine-readable evidence](../scripts/rust-integration/m7-d2c5e611-amd64-hdhr-configured-client.json)

Full credit belongs to the original Dispatcharr authors and contributors and the authors of its dependencies.
