#!/bin/sh
set -eu
export LC_ALL=C TZ=UTC PATH=/usr/local/bin:/usr/bin:/bin
unset LD_LIBRARY_PATH LD_PRELOAD
prefix=/opt/dispatcharr-comskip
docs=/usr/share/doc/dispatcharr-standalone-comskip
loader="$prefix/lib/ld-linux-aarch64.so.1"
test "$(id -u)" = 65532
test "$(uname -m)" = aarch64
test "$(dpkg --print-architecture)" = arm64
test ! -e /dispatcharrpy
work=$(mktemp -d /tmp/owned-comskip.XXXXXXXX)
trap 'rm -rf -- "$work"' EXIT
cd "$work"
"$loader" --inhibit-rpath "" --inhibit-cache --library-path "$prefix/lib" --list "$prefix/bin/comskip" > loader.txt
help_status=0
/usr/local/bin/comskip --help > help.stdout 2> help.stderr || help_status=$?
test "$help_status" = 2
grep -m1 -E '^Comskip 0\.83\.001' help.stderr > version.txt
printf 'private-loader-and-help=complete\n' >&2
/usr/local/bin/ffmpeg -hide_banner -v error -f lavfi -i 'testsrc2=size=320x240:rate=25' \
    -f lavfi -i 'sine=frequency=450:sample_rate=44100' -t 8 -threads 1 -c:v mpeg2video -c:a mp2 -f mpegts owned.ts
sha256sum owned.ts > before.sha256
/usr/local/bin/ffprobe -v error -show_entries stream=codec_name:format=duration -of json owned.ts > shape.json
/usr/local/bin/ffmpeg -hide_banner -v error -i owned.ts -map 0:v:0 -map 0:a:0 -f null -
printf 'owned-audio-video-decoded=complete\n' >&2
detector_status=0
/usr/local/bin/comskip --ini=/app/docker/comskip.ini owned.ts > comskip.stdout 2> comskip.stderr || detector_status=$?
printf 'owned-detector-exit=%s\n' "$detector_status" >&2
test "$detector_status" = 1
test -f owned.edl && test ! -s owned.edl
sha256sum --strict --quiet -c before.sha256
printf 'empty-edl-and-input-unchanged=complete\n' >&2
printf 'BEGIN manifest\n'
cat "$docs/unit-inputs.json"
printf 'END manifest\nBEGIN files\n'
find "$prefix" "$docs" -type f ! -name unit-inputs.json > files.txt
LC_ALL=C sort files.txt > sorted.txt
while IFS= read -r file; do sha256sum "$file"; done < sorted.txt
sha256sum /usr/local/bin/comskip /app/docker/comskip.ini
printf 'END files\nBEGIN loader\n'
cat loader.txt
printf 'END loader\nBEGIN version\n'
cat version.txt
printf 'END version\nBEGIN media\n'
cat shape.json
printf 'END media\nBEGIN media-digest\n'
cat before.sha256
printf 'END media-digest\n'
