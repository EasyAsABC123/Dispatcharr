# Actual Python playback characterization

```sh
python3 scripts/rust-playback-baseline/run.py \
  --output /tmp/dispatcharr-playback --viewers 1 10 --trials 1
```

Requires Git, Python 3.9+, tar, Docker Engine, and Compose v2. Docker must be on
PATH or supplied with `--docker /absolute/path/to/docker`. The runner honors
`DOCKER_HOST` and `DOCKER_CONFIG`. Choose a new empty output directory every run.
The default viewer list is 1/10/100 and default trials is three; the verified
initial probe intentionally used only 1/10 with one trial each. This is short
characterization, not a completed milestone or a Python/Rust comparison.

The source is archived from immutable baseline commit
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and mounted read-only. Both services
use the pinned linux/amd64 CI image in `docker/compose.rust-playback.yml`.
Source requirements must match the image. No dependency installation occurs.

An ephemeral PostgreSQL17 database is initialized as UTF-8 and migrated with
the baseline's migrations. Redis runs inside the baseline container. A real ORM
seed uses the migrated custom M3U account, its active default unlimited profile,
the locked native Proxy stream profile, and a fixed channel UUID. The stream URL
points exclusively to the Compose origin. No application source is modified.
The disposable network policy allows private test networks and localhost only.

The backend uses the existing WSGI application and production gevent monkey
patching under one uWSGI worker/400 async slots. This dedicated configuration
does not start Celery, Daphne, nginx, or scheduled provider imports. It measures
the real live proxy, media Redis buffers, client generators, and cleanup, while
keeping the fixture deterministic. It does not represent full production idle
memory or frontend/browser resource use.

The origin generates a valid 180-second MPEG-TS fixture once with image FFmpeg:
320x180 MPEG2 video at 25 fps, MP2 audio at 48 kHz, and a 2 Mbps mux. It serves packet-
aligned bytes at 250,000 bytes/second and exposes connection/byte counters. Fixture
SHA256 is saved. The runner decodes three seconds of actual proxied playback
through FFmpeg and requires video frames before measuring resources. Longer
soaks require a continuous timestamp-stable muxer or a longer clip: the current
origin repeats its timestamps after 180 seconds.

Docker uses a unique bridge/project with ports published only on 127.0.0.1.
The network is not marked `internal`: this rootless Docker environment otherwise
does not publish its ports. No external provider URL is configured, but egress
blocking is not enforced by this Compose file. No production service or volume is
mounted. The runner collects logs and removes only its own project in `finally`;
the source snapshot remains alive until cleanup. Commands have finite timeouts.

Each trial uses `scripts/rust-perf/perf.py` with 2 seconds warmup, 10 seconds
measurement, and 1 second sample interval. It samples both the entire baseline
container (including PostgreSQL/Redis/media tools) and the origin. Evidence includes
tool hashes, exact commands, source archive hash, image digest, fixture hash,
decoder progress, per-viewer data/errors, resource samples, and upstream counters.

`verified-playback.json` records the successful initial local 1/10-viewer probe.
It had no load errors, empty responses, missing-data viewers, or sampling errors.
The short duration and startup buffer catchup make this insufficient for steady-
state conclusions. Do not infer application gains from these measurements; there
is no Rust playback candidate yet. Freeze longer warmups/durations, collect at
least three trials and 100-viewer runs, verify upstream sharing and decoded output,
then compare an equivalent Rust route on identical hardware/fixture/settings.
