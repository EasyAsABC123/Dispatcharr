# AMD64 DVR Library read response parity

The ordinary Linux AMD64 package at
`48181ab0067d85c628bab46a93af717c1b5759c0` matched all ten signed Original
DVR Library and Guide Series Rules GETs. Both implementations returned HTTP
200, matching typed JSON, `Allow`, `Vary`, `Content-Length` and media
type. All ten pairs had the same raw response length and SHA-256.

| Read scope | Signed observations | Exact raw body matches |
| --- | ---: | ---: |
| Recording list and JSON suffix | 4 | 4 |
| Recording detail and JSON suffix | 4 | 4 |
| Guide Series Rules GET | 2 | 2 |

The Standard viewer saw one recording in the assigned channel profile. The
admin saw both completed recordings. Both roles could read the shared detail
and series rule. Rust now serializes the recording `channel` field after
`custom_properties`, resolving the eight byte-level differences retained on
`009cc8dd`. [Earlier typed comparison](rust-m0-m5-009cc8dd-amd64-dvr-library-reads.md)

The candidate ran fresh against disposable owned fixture data. The
comparison reused the pinned Original responses and their independent
cleanup result. The candidate replay retired the stock web process before
Rust served the same GETs. Each arm preserved the selected PostgreSQL rows,
scheduler rows and broker state. All five owner phases exited 0 on both
arms. Two Original and four candidate fixture role generations retired.
Each owner reported zero remaining containers, networks and volumes.

The candidate manifest SHA-256 was
`18b677fe7d7fa5ddd482e6b6892e95999e93a0a2db8cdec3f8cc82b5baf020c9`.
The verified image was
`sha256:c0d7a5a98311ffe8becb1a8e729a1dbee4e1fd1704b212cab5826ade45fabb3e`.
The retained Original used revision
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.
The proof covers four declared GET routes and GET on the mixed-method
Series Rules row. Writes, playback, performance and full M0 and M5
acceptance remain open outside this scope.
[Scoped evidence](../scripts/rust-integration/m0-m5-48181ab0-amd64-dvr-library-wire.json)

The original Dispatcharr authors and contributors retain full application
and protocol credit.
