#!/bin/sh
# Use the privately built upstream ARM64 final stage, with this script and
# /expected read-only. Stdout is a bounded text receipt.
set -eu
export LC_ALL=C PATH=/usr/local/bin:/usr/bin:/bin
test "$(id -u)" = 65532
test "$(uname -m)" = aarch64
tmp=$(mktemp -d)
stage=notice-inputs
trap 'status=$?; if [ "$status" -ne 0 ]; then printf "guest-stage=%s\n" "$stage" >&2; fi; rm -rf "$tmp"' EXIT
doc=/usr/local/share/doc/dispatcharr-ffmpeg
cmp "$doc/CONFIGURATION" /expected/CONFIGURATION
cmp "$doc/UBUNTU-PACKAGES.txt" /expected/UBUNTU-PACKAGES.txt
(
    cd /
    sha256sum -c --status /expected/notices.sha256
)
for tool in ffmpeg ffprobe; do
    stage="$tool-elf-and-sonames"
    file=/usr/local/bin/$tool
    test "$(od -An -tx1 -j 4 -N 3 "$file" | tr -d ' \n')" = 020101
    test "$(od -An -tx1 -j 18 -N 2 "$file" | tr -d ' \n')" = b700
    "$file" -version > "$tmp/version"
    grep -q "^$tool version 9[.]0[.]2-Dispatcharr " "$tmp/version"
    configure=$(sed -n 's/^[[:space:]]*configuration:[[:space:]]*//p' "$tmp/version")
    test "./configure $configure" = "$(tail -n 1 /expected/CONFIGURATION)"
    ldd "$file" > "$tmp/ldd"
    ! grep -q 'not found' "$tmp/ldd"
    awk '$2 == "=>" {print $1} /^[[:space:]]*\/lib\// && $2 != "=>" {name=$1; sub(/^.*\//,"",name); print name}' "$tmp/ldd" |
        sort -u > "$tmp/sonames"
    cmp "$tmp/sonames" /expected/sonames.txt
    digest=$(sha256sum "$file" | cut -d ' ' -f 1)
    printf '%s=%s\n' "$tool" "$digest"
done
stage=generated-encode
ffmpeg -hide_banner -v error -nostdin -f lavfi -i testsrc2=size=160x90:rate=25 \
    -f lavfi -i sine=frequency=1000:sample_rate=48000 -t 8 -c:v libx264 -threads 1 \
    -preset ultrafast -c:a libfdk_aac -f mpegts "$tmp/fixture.ts"
stage=generated-probe
ffprobe -v error -show_entries stream=codec_type -of csv=p=0 "$tmp/fixture.ts" |
    sed '/^$/d;s/,$//' | sort -u > "$tmp/types"
printf 'audio\nvideo\n' > "$tmp/expected-types"
cmp "$tmp/types" "$tmp/expected-types"
stage=generated-decode
ffmpeg -hide_banner -v error -nostdin -xerror -i "$tmp/fixture.ts" \
    -map 0:v:0 -map 0:a:0 -f null -
printf 'runtime_package_versions=25\nshared_sonames_per_tool=55\nnotice_hashes=%s\n' \
    "$(wc -l < /expected/notices.sha256 | tr -d ' ')"
printf 'configure_and_media_workflow=passed\n'
