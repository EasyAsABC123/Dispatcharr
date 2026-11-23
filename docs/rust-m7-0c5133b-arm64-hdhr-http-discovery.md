# M7 ARM64 configured HDHomeRun HTTP discovery

Pinned Original and Rust application `0c5133b62dc9f2df0900f2228e25b75baecb6754` both passed the same configured HDHomeRun client workflow on Lappy. Each independent Compose project had one configured device and one channel. The client requested `/hdhr/discover.json` and `/api/hdhr/discover.json` and followed the advertised `/hdhr/lineup.json` address from each response. Both implementations returned HTTP 200, the same selected headers and the same typed device and lineup fields, including channel number 12 and its advertised stream address.

Both fixtures left their eight observed database tables and all 184 migration records unchanged. The application and PostgreSQL process generations stayed stable through each workflow. Both application and probe exits were 0. The pair and independent teardown audit passed. All 12 owned containers, two networks and four volumes were verified absent. Neither fixture published host ports.

The three earlier fuller fixture attempts ended before either application probe. The first lacked the seed exception and had unreadable transferred guest scripts. The next two recorded duplicate auto-created memberships and an explicit primary key rejected by the OutputProfile create path. All three retired their resources. The passing pair qualifies configured HTTP discovery and the advertised lineup. Named and output profiles, scan status, XML, automatic LAN discovery, playback, and performance remain outside this result.

The original Dispatcharr authors and contributors retain full credit. [Machine-readable evidence](../scripts/rust-integration/m7-0c5133b-arm64-hdhr-http-discovery.json)
