# Current DVR case4 retained MKV codec review

The retained `TV_Shows/Owned Parented Artwork/S01E01.mkv` was read without replaying a recording, task or application. Its 980,283 bytes matched the supplied descriptor SHA-256 `a65b9199bbc047f4009a9ddbd3cac6b44e45ffbd705f53a491f12bb7b68928ef` before copying and after the review. The original file was not modified. Only a byte-identical owned copy, mode 0444, was mounted into each sidecar.

Both sequential sidecars used pinned Original image `ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f`, network `none`, read-only root filesystem, UID/GID 65532, all capabilities dropped, no-new-privileges, 128 MiB memory and 64 PIDs. Each had exactly one read-only bind mount. No Source web/worker, PG, Redis, provider or media origin was started.

`ffprobe` exited 0 and reported Matroska/WebM format, format duration **27.480000 seconds**, MPEG-2 Main-profile video, **160 × 90**, `yuv420p`, nominal and average frame rate **25/1**. Stream duration and frame-count fields were unavailable; no decoded-frame count is inferred from duration × frame rate.

The full video decode used:

```text
ffmpeg -threads 1 -filter_threads 1 -v error -i /tmp/owned-recording.mkv -map 0:v -f null -
```

It exited 0 with empty error stderr. There was no seek, time limit, frame limit or prefix-only read in the codec arguments; all selected video input was processed to EOF. Audio decoding was not requested or claimed.

`execution.json` retains all 24 actual CLI receipts, bounded stdout/stderr hashes, PID/start identities and wait/pipe settlement. The sidecars exited normally, `docker wait` returned 0, and each was removed before the next ran. The host controller completed in 0.913917 seconds under its fixed 60-second total bound (45 seconds work plus 15 seconds cleanup reserve), with a 1 MiB in-flight cap per stream.

`independent-review.json` rechecks 124 predicates, including the actual 48 command-stream hashes, codec arguments/metadata, unchanged source/copy hashes and seven fresh exact project/CID/name cleanup queries. All were empty and physically settled. No global Docker-idle claim is made; another owned DVR fixture could run independently.

This is retained Native case4 artifact readability and full-video decode evidence. It does not establish Source duration equality, recording scheduling correctness beyond the existing case, resource performance or a causal latency gain. Raw metadata, inspect records and the owned MKV copy remain private review artifacts, not an automatic public payload.
