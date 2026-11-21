#!/bin/sh
set -eu
export LC_ALL=C TZ=UTC PATH=/usr/local/bin:/usr/bin:/bin HOME=/tmp XDG_CONFIG_HOME=/tmp
unset LD_LIBRARY_PATH LD_PRELOAD VLC_PLUGIN_PATH
[ "$(id -u):$(id -g)" = 65532:65532 ]
[ "$(uname -m)" = aarch64 ] && [ "$(dpkg --print-architecture)" = arm64 ]
[ ! -e /dispatcharrpy ] && [ ! -e /verified-vlc ] && [ ! -e /verified-ffmpeg ]
cd /tmp
vlc --ignore-config --version > vlc.version
cvlc --ignore-config --version > cvlc.version
[ "$(head -n 1 vlc.version)" = "$(head -n 1 cvlc.version)" ]
ffmpeg -version > ffmpeg.version
ffprobe -version > ffprobe.version
printf 'checkpoint: wrappers\n' >&2
ffmpeg -hide_banner -v error -f lavfi -i 'testsrc2=size=320x240:rate=25' \
    -f lavfi -i 'sine=frequency=450:sample_rate=44100' -t 8 -threads 1 -c:v mpeg2video -c:a mp2 -f mpegts owned.ts
printf '%s  owned.ts\n' 562fa93972763130c3079330b5e028a1e248ff98d59202129164d20a8c67d0b1 | sha256sum --strict --quiet -c
printf 'checkpoint: input-generated\n' >&2
timeout 60 cvlc --ignore-config --no-plugins-cache --no-video-title-show --play-and-exit --file-caching=100 \
    /tmp/owned.ts --sout '#standard{access=file,mux=ts,dst=/tmp/remux.ts}' > vlc.remux.stdout 2> vlc.remux.stderr
[ -s remux.ts ] && [ "$(wc -c < remux.ts)" -le 8388608 ]
printf 'checkpoint: remuxed\n' >&2
ffprobe -v error -count_frames -show_entries stream=codec_name,codec_type,width,height,nb_read_frames:format=duration -of json remux.ts > remux.json
ffmpeg -hide_banner -v error -xerror -i remux.ts -map 0:v:0 -map 0:a:0 -f null -
printf '%s  owned.ts\n' 562fa93972763130c3079330b5e028a1e248ff98d59202129164d20a8c67d0b1 | sha256sum --strict --quiet -c
printf 'checkpoint: decoded\n' >&2
vlc_prefix=/opt/dispatcharr-vlc
docs=/usr/share/doc/dispatcharr-standalone-vlc
printf 'BEGIN inventory\n'
cat "$docs/unit-inputs.json"
printf 'END inventory\nBEGIN files\n'
find "$vlc_prefix" "$docs" /usr/lib/aarch64-linux-gnu/vlc/plugins /usr/share/vlc -type f ! -name unit-inputs.json -print | sort |
    while IFS= read -r name; do sha256sum "$name"; done
sha256sum /usr/local/bin/vlc /usr/local/bin/cvlc
printf 'END files\nBEGIN versions\n'
head -n 1 vlc.version
head -n 1 ffmpeg.version
head -n 1 ffprobe.version
printf 'END versions\nBEGIN media\n'
cat remux.json
printf 'END media\nBEGIN bytes\n'
wc -c < remux.ts
sha256sum remux.ts
printf 'END bytes\n'
