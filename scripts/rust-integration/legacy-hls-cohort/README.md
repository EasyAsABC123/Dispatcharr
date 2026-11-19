# Legacy HLS playlist descriptor parity

The fixture runs one authenticated, explicitly profiled HLS viewer against pinned Original Dispatcharr and immutable Rust Main in separate owned Compose stacks. Dispatcharr and its original authors retain full credit for the application and reference behavior.

The viewer warms the playlist, decodes an audio and video segment, and reads the same ordinary capability URL against four exact descriptor variants. Current, timestamp-absent, and advertised-target-absent descriptors must return 200 and the exact playlist bytes produced by the pinned Original functions. An aged descriptor must return 404 with `Playlist stale`. Redis compare-and-set preserves the absolute expiration and restores the original bytes. An ownership, publisher-byte, or expiration change prevents a restore from overwriting newer state. Each seven-second origin hold must show zero movement during the measured read.

The viewer must decode a newer segment after the restore and retain one upstream connection, one profiled client, and the same HLS owner and FFmpeg process generation. The fixture checks the normal cache headers, stops the viewer through the authenticated route, verifies capability retirement and client absence, and attests the absence of its containers, networks, volumes, and original process generations. This is a functional scenario with no M3 milestone or performance result.

## Prepare and verify offline

From a clean checkout with the pinned Git revisions locally available, set `work` to a new directory and optionally set `build_receipt` to an immutable Main build receipt.

```bash
fixture=scripts/rust-integration/legacy-hls-cohort
python3 -B "$fixture/run.py" --prepare-only --output "$work"
python3 -B "$fixture/run.py" --verify-export --output "$work" --build "$build_receipt"
python3 -B "$fixture/test_offline.py" "$work"
```

The exporter materializes 21 exact support files from the pinned support revision in [support.json](support.json). Git generates and verifies the byte-identical Original source archive from `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The fixture verifies all 1,211 Original file digests when bootstrapping. The destination holds generated inputs and run output. The checkout needs no retained captures, copied Original archive, or private command logs. The offline tests require Lua and exercise finite and persistent expiration controls, competing Redis owners and publishers, and all four Original render outcomes.

## Run the immutable pair

Run with the already-owned local Unix Docker daemon, the pinned Original image already loaded and a candidate package already loaded. The commands below use the default Docker CLI from `PATH`. Use `--docker` to select an existing CLI if needed. Redis in the Original image must support `PEXPIRETIME` and `SET … KEEPTTL`.

```bash
export DOCKER_HOST=unix:///path/to/owned/docker.sock
python3 -B "$fixture/run.py" --build "$build_receipt" --output "$fresh_run_directory"
```

The launcher uses the shared dispatcher lock and one 600-second functional deadline followed by separately bounded cleanup. Share only the top-level report and the sanitized application report. The run directory also holds raw Redis snapshots and private fixture logs.
