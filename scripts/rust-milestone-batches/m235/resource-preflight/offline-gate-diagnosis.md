# Offline diagnosis and future protocol review (no rerun)

The failed phase and frozen gates remain unchanged. No application, Source model
or measurement was rerun for this diagnosis. Host FFprobe inspected only retained
capture files, bounded30seconds per invocation; these observations are not the
omitted in-fixture decoder gate and do not change the failed result.

The origin source is **constant mux rate**, not simply a1Mbps output promise:
`fixture_command` creates H264/AAC media with nominal video1Mbps/audio128kbps,
then `-muxrate2000000`. `RATE=250000` and1316byte writes pace the padded input at
250,000bytes/s. The observed origin advance was7,499,884bytes in29.998744seconds,
consistent with that independently declared rate. The active custom FFmpeg
profile uses `-c copy ... -f mpegts pipe:1`, without an output muxrate. It remuxes
encoded content and does not promise retention of input null stuffing. Both
captured output readers report media_bytes==bytes (no null packets counted).
Therefore assuming5MB output in30s from the2Mbps padded origin is unjustified.
The encoded/remuxed output is variable-rate and smaller than the padded input.

Pinned `apps/proxy/config.py` sets `BUFFER_CHUNK_SIZE=188*1361=255868` bytes.
Retained reader increments occur in exactly those255868byte batches. The reader
counter updates after actual `response.read1(65536)` calls and packet alignment,
not a1-second publish timer. Its packet accounting is synchronized continuously;
the1-second observer legitimately sees unchanged counters between real batches.
The exact per-layer cause of batching is not fully instrumented, but the observed
batch size matches the Source Redis buffer configuration. At observed useful
rates, one such batch spans roughly1.6seconds, so every-tick advancement is an
unsupported requirement even while media is usable. No unobserved guarantee of
socket liveness is inferred from that explanation.

Whole-file FFprobe packet parsing with packet file positions avoids starting a
new parser mid-GOP. Restricting packet observations to the recorded byte offsets
for the30second measurement yields video PTS coverage29.04seconds/726packets for
owner and30.68seconds/767packets for follower, with14/15video keyframe packets.
Audio position-bearing packets cover28.864/30.571seconds. Full-file FFprobe exit0
has empty stderr. Different boundary chunks explain why byte totals may differ
by255868 bytes despite similar PTS work. These are packet/PTS diagnostics, not a
claim that every frame decoded or that the failed phase now passes. A separate
initial skip-offset probe emitted expected missing-PPS warnings when starting
mid-GOP; its raw output is retained and is superseded only for this diagnostic by
whole-file position filtering, not erased.

A future protocol should be reviewed and frozen before any new execution:

1. Independently index the deterministic input fixture’s stream PTS, frame rate,
   keyframes and encoded-packet hashes/bytes. Keep padded input bytes distinct
   from remux output bytes. Establish remux-safe encoded content comparison;
   transport headers/null stuffing are not equivalent useful work.
2. Keep the30second resource window and1second raw accounting. Observe byte
   movement at each tick but qualify bounded **rolling** progress using the
   independently specified buffer capacity and media PTS cadence, with an
   explicit maximum stall budget. Do not require a batch on every tick.
3. Persist whole-window capture offsets, raw reader totals and final states.
   Parse the complete capture for valid codec initialization, then associate
   packets with the window using positions/PTS; require audio+video PTS coverage
   within a predeclared boundary-chunk tolerance of the real window. Reject
   timestamp gaps/regressions and compare encoded packets with the input index.
4. Decode full captures off-window on every phase, including invalid phases.
   Require useful decoded video/audio and independently bound decoded duration;
   preserve decoder errors. Do not put decode CPU inside streaming measurements.
5. Compare equal encoded useful content/media duration across fixed O,N,N,O
   windows. Report whole-window bytes, decoded/PTS duration and boundary slack
   beside resource totals. Avoid an arbitrary5MB floor or5% output-byte equality
   rule until independent fixture content and transport boundaries justify them.

This proposes stronger observable media correctness instead of merely lowering
thresholds. It does not authorize a rerun, change current gates, accept fullM3,
or establish any CPU/memory gain.
