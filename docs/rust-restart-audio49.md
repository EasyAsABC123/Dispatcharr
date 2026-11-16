# Audio49 restart media diagnosis: both strict workflows failed

The fresh complete Original→Native `nonempty-restart` pair exited **1 after
481.115 seconds**. Both backends failed the unchanged decoder-stderr criterion.
Decoder exit zero does not override that failure. Source failure did not prevent
independent Native collection after safe cleanup. No runtime arguments, Origin
behavior, configured grace period, cache state or acceptance thresholds changed.
This supplies diagnostic evidence, not milestone or performance acceptance.

Original Dispatcharr and dependency authors retain all application, library and
algorithm credit. Original revision is `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`;
Candidate37 revision is `da75542b6f46ea982dfefb75b5541757e78cd34a`, image
`sha256:83edf4700ef2be10b6812c8045d0d5fca94216861854067465dd36154f83bb88`.

## Actual stage observations

The observer retained recording3's stable, playlist-listed segments before the
real worker crash and after the first completed resumed segment. Both backends
recorded no observer errors and 9,322,280 bytes of media-content reads. Playlist,
process and log observations have separate bounded scopes; this is not a total
I/O measurement. A stable listed segment is not independent proof of kernel file
descriptor closure. The Origin prefix is an actual fixture-file prefix, not a
capture of bytes delivered through the proxy.

The qualified probes found:

| Stage, each backend | AAC packets | Timestamp observation |
|---|---:|---|
| Origin TS prefix | 586 | PTS/DTS increase; PTS1.400–13.880s |
| First precrash TS | 189 | PTS/DTS increase; PTS1.400–5.410667s |
| Second precrash TS | 187 | PTS/DTS increase; PTS5.432–9.400s |
| First resumed TS | 189 | PTS/DTS increase internally; resets to1.400s |
| Final MKV7–10s window | 144 | Three distinct packets share PTS/DTS8.000s |

Both actual resumed playlists contain `EXT-X-DISCONTINUITY` before the first new
segment. A timeline reset is therefore present and declared at the HLS restart
boundary. Equal DTS first appears in the observed final MKV, while individual
probed TS segments increase internally. This does not yet isolate HLS demux
adjustment, ffmpeg CLI timestamp processing or MKV mux behavior.

All144 final-window packets match across Source and Native in PTS, DTS, duration,
size and data hash. The equal-time triplet has sizes343/437/372 bytes and distinct
hashes. This comparison uses the same MKV representation. TS AAC packets can
include ADTS headers while MKV exposes raw AAC; differing cross-container hashes
cannot establish payload loss or identity without validated elementary-payload
canonicalization. [Stage observations](../scripts/rust-integration/restart-audio49/stage-observations.json)
preserve the scoped witnesses and raw-output hashes.

## Permission control and physical cleanup

The first ten probes retained eight real intermediate-file permission failures
under container UID1000, and two successful final-MKV probes. An isolated
read-only, network-disabled control then proved Docker was rootless: guest UID0
maps to host UID1000, the owner of the private mode0600 copies. Guest UID1000 maps
to a subordinate host UID. Guest UID0 read188 bytes successfully with all
capabilities dropped and no new privileges. No production privileges or file
modes changed. A fresh corrected batch ran only the eight intermediates under
the proved file-owner mapping; all eight passed. The two final probes were not
repeated. Original failed receipts remain intact.

All18 initial media-job inputs retained their before/after identities and hashes; their
process groups settled and output pipes reached EOF. All18 job containers and
the isolated permission-control container are absent. Independent application
cleanup also verified both projects empty, all six exact application containers
absent, and all six original positive host PIDs absent. Application/controller
EOF and process-group settlement are recorded separately; no independent
PostgreSQL-session retirement claim follows from these receipts.

## Six follow-up packet controls

Two HLS demux probes used the same pinned image and actual remux input flags on
diagnostic copies of the three-segment playlists, with ENDLIST added only to
those copies. Each yielded375 packets, seven without DTS. Among adjacent packets
with known DTS, the9.400→1.400s reset remained; no equal-DTS pair appeared. The
first equal-DTS collapse remains the one observed in the final MKV. Missing
DTS values prevent excluding every demuxed packet; ffmpeg CLI timestamp
adjustment and MKV mux behavior remain unseparated.

Four additional TS `show_data` probes supplied752 actual packets. The parser
checked decoded size and raw packet SHA, ADTS sync/layer, exact frame length and
one raw block before stripping the structurally identified header. Every packet
validated. The first two resumed raw AAC payloads,437 and372 bytes, match the
second and third final8.000s packets in size and SHA. The last precrash payload,
343 bytes, also matches the first final packet, but that same sine-fixture AAC
payload recurs earlier: a matching hash alone does not establish unique temporal
origin. No CRC integrity or decoded-audio equivalence is claimed.

All six commands exited zero in2.283 seconds total, with input identities/hashes
unchanged, settled process groups, output EOF, and all six exact containers
absent. Together with earlier probes/control, all25 media/control containers are
absent. Source and Native strict workflow failures remain unchanged. See the
[six-probe receipts](../scripts/rust-integration/restart-audio49/followup-six-probes.json).

The [packet index](../scripts/rust-integration/restart-audio49/index.json) binds
compact application, cleanup, permission and media projections plus raw evidence
hashes. Full owned URLs, task payloads and worker logs remain private. These
diagnostics cannot turn the retained strict failures into passes. A usable paired media
baseline remains required before full equivalent-work performance acceptance.
