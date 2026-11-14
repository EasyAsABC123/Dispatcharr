# Captured 61-second recording component

This packet retains one actual Original 61-second video/audio recording, a Native normal run, seven pre-ACK refusals and a held result reply. These are three distinct Native source epochs; the held run alone used the combined 892-input build. Full source snapshots, maps, commands, executed tools, media, clocks, raw IPC and six failed Native contexts are retained. Nothing here activates Main, a registry or default workers, or completes M6.

Original whole MKV duration was 56.64 seconds. Native normal and held runs each produced 57.68 seconds. Native first-observed capture latency was 2.209062 and 2.079771 seconds respectively. Acceptance was declared before output: latency <=15 seconds, whole duration 45..63 seconds, and full video/audio EOF decode. These are selected fixture criteria, not duration equality, a general SLA or a performance result.

The active marker remains literal `1`, EX45. The explicit 61-second path advances a move-only confirmed receipt only after an actual same-origin integer-one renewal with causal clocks inside the preceding 45-second interval. Four renewals and final integer-one removal were observed. The existing 30-second entry remains separate. Foreign same-value replacement and general recovery remain open.

The held result run retained the original owner and exhausted single slot while the actual result reply was held; afterwards its conservative ambiguous-result disposition still pins the owner until owned process exit. This is not a recoverable long-running worker or a claim that all resources drain while the process remains alive. Original AsyncResult nevertheless observed the actual SUCCESS/null backend value.

Six earlier contexts retain HOST1: the initial normal/result probes did not retain the operation outcome; initial refusals had a profile classification mismatch; the second normal exposed expiry of unchanged acquisition authority; the second result never reached its target; the third result reached the hold but exceeded the unchanged 2 MiB cumulative report cap. No failed process was relabeled. The final peer relocates only repeated stderr tails into at most two exact content-addressed 65536-byte files; strict SHA/size/path restoration reproduces every raw IPC sample before acceptance. Raw stdout remains capped at 1 MiB.

The Source observer originally performed two independent ZRANGEs. An actual pinned-image serializer-only witness matched all 282 retained Source audio/prime pairs. That resolves these retained associations only; the Native collector uses one raw read and derives decoded values from it. Original prime HOST1/monitor overflow remains separate and is not promoted by this audio evidence.

Restore without executing applications:

```sh
python scripts/rust-job-contracts/recording-audio61/materialize.py --output /tmp/recording-audio61-evidence
python scripts/rust-job-contracts/recording-audio61/recompute.py --evidence /tmp/recording-audio61-evidence
python scripts/rust-job-contracts/recording-audio61/test_public.py
```

Restoration accepts only the projected hash, never the private raw hash as a fallback. Only ten exact owned Origin control nonces become `xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`; all other bytes/order/statuses remain unchanged. Private originals and projection hashes stay retained. The restored historical commands are evidence, not dispatch authorization. Outside ELF bytes are excluded; exact hashes and completed build receipts remain. Full frozen Rust input bytes are included. Original source is bound to Git 1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430 and its 14 actual module hashes; support 6148 inputs can be reconstructed using committed `scripts/rust-integration/image54-checkpoint/reconstruct.py --kind support --repo "$PWD" --output /tmp/audio61-support-f0`. No live WIP or implicit HEAD is a build input.
