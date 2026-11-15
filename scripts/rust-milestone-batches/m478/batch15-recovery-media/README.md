# Pinned read-only Source/Native recovery-media review

One Source34f901 utility container ran the pinned FFmpeg/ffprobe against six immutable Source/Native recordings3/4/5 through stdin, one utility decoder at a time. Both3 recordings decoded positive A/V with exit0, but produced the same equal-audio-DTS null-mux warning. Each contains three distinct adjacent audio packets at8s; no strict backwards transition was observed. Original strict no-stderr failures remain false. The separate timestamp-warning-only category is explicit and does not retroactively pass old whole contexts.

Previously unreached4/5 checks were newly executed on both backends: exit0, stderr0 and positive A/V frames. A real interior32KiB deletion emitted Matroska/demux errors despite exit0 and was rejected; backwards, codec-error, nonzero-exit and wrong-stream controls also reject.

The tool used network-none, read-only root, UID/GID65532, cap-dropALL,1CPU and512MiB. All decoder commands/EOF/groups settled. Only after tools finished, the idle sleep keeper was explicitly killed137 and the exact container/host generation removed; no OOM or app-shutdown success is implied. No app, DB/cache/API/source bootstrap, network or benchmark was run. Original Dispatcharr/dependency authors retain full credit; all original media files retain their exact hashes.
