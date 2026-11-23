# AMD64 DVR Library and Guide Series Rules reads

On the ordinary Linux AMD64 package at
`009cc8dda306ba1f4848ab937107dc2722f84f5b`, all ten signed DVR reads
matched Original's HTTP status, media type, typed JSON, `Allow`, `Vary`
and `Content-Length`. The Standard DVR viewer saw one recording in the
assigned channel profile. The admin saw both completed recordings.
The roles each read the same accessible detail and the same series rule.

| Route scope | Matched typed observations | Matching raw bodies |
| --- | ---: | ---: |
| Recording list and its JSON suffix | 4/4 | 0/4 |
| Recording detail and its JSON suffix | 4/4 | 0/4 |
| Guide Series Rules GET | 2/2 | 2/2 |

Both recording response forms contain the same fields and values. Original
writes `channel` after `custom_properties`. Rust writes `channel` after
`id`. Independent reconstruction reproduced the retained raw SHA-256 for
both implementations on all eight recording responses. The existing
typed scope passed and the raw field-order difference remains open.

The fixture used isolated synthetic completed recordings and one series
rule. Both runtime arms left the selected PostgreSQL rows, scheduler rows
and broker state unchanged. The Original owner retired two exact role
generations. The Rust pair retired four, including its initialization
container. All owned container, network and volume inventories were empty.
Both owners' preflight, run, cleanup, proof and campaign phases exited 0.

The exact verified ordinary manifest SHA-256 was
`13f60786980097f8dd71fa95890e24baa24eed23da293b4929c94b6953aa2862`.
The Original source revision was `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.
This verifies four declared GET routes and a GET observation on the
mixed-method Series Rules register row. Formal M0 and M5 acceptance,
writes, playback and comparative performance remain separate.
[Scoped evidence](../scripts/rust-integration/m0-m5-009cc8dd-amd64-dvr-library-reads.json)

The original Dispatcharr authors and contributors retain full application
and protocol credit.
