#!/usr/bin/env bash
set -euo pipefail
umask 077
export LC_ALL=C TZ=UTC SOURCE_DATE_EPOCH=1744985293
unset LD_PRELOAD LD_LIBRARY_PATH PKG_CONFIG_PATH PKG_CONFIG_LIBDIR PKG_CONFIG_SYSROOT_DIR
scope=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
checkout=$(git -C "$scope" rev-parse --show-toplevel)
output=${RUNNER_TEMP:?}/native-arm64-comskip-evidence
mkdir -- "$output"
work=$(mktemp -d -- "$RUNNER_TEMP/native-arm64-comskip.XXXXXXXX")
phase=host
finish() {
    result=$?
    printf '%s\n' "$result" > "$output/actual-exit"
    printf '%s\n' "$phase" > "$output/terminal-phase"
    rm -rf -- "$work"
    return "$result"
}
trap finish EXIT

revision=a140b6ac8bc8f596729e9052819affc779c3b377
parent=d905583ee891260684ecb126a5c5427016c15383
[[ ${GITHUB_REPOSITORY:?} == EasyAsABC123/Dispatcharr ]]
[[ ${GITHUB_REF:?} == refs/heads/validation/m10-arm64-comskip-a140b6ac-2 ]]
[[ $(git -C "$checkout" rev-parse HEAD) == "${GITHUB_SHA:?}" ]]
[[ $(git -C "$checkout" rev-parse HEAD^) == "$parent" ]]
expected=$(printf '%s\n' .github/workflows/native-arm64-comskip.yml \
    scripts/rust-platform-contracts/native-arm64-comskip/{project.py,qualify.sh,source-tree.json,verify_source.py})
[[ $(git -C "$checkout" diff-tree --no-commit-id --name-only -r HEAD) == "$expected" ]]
[[ $(uname -s) == Linux && $(uname -m) == aarch64 && ${RUNNER_ARCH:?} == ARM64 ]]
[[ $(dpkg --print-architecture) == arm64 && $(gcc -dumpmachine) == aarch64-linux-gnu ]]
[[ $(id -u) -ne 0 ]]
# shellcheck disable=SC1091
. /etc/os-release
[[ ${ID:?} == ubuntu && ${VERSION_ID:?} == 24.04 ]]
gcc -dumpfullversion > "$output/compiler-version"
for tool in autoconf automake autoreconf make readelf pkg-config python3 curl ldd gpgv; do
    command -v "$tool" >/dev/null
done
printf '%s\n' "$GITHUB_SHA" > "$output/validation-commit"
printf '%s\n' "$parent" > "$output/validation-parent"

phase=source
curl --fail --show-error --silent --location --proto '=https' --proto-redir '=https' \
    --max-time 40 --max-filesize 1048576 \
    "https://codeload.github.com/erikkaashoek/Comskip/tar.gz/$revision" -o "$work/upstream.tar.gz"
recipe=$checkout/rust/runtime/comskip-Original-DispatcharrBase
config=$checkout/rust/runtime/comskip-source.ini
printf '%s  %s\n' 7614488f8addcd44850f524f5a7d104527aa6d80c25dbdf8d60219d4e658b896 "$config" | sha256sum --strict -c --quiet
python3 -B "$scope/verify_source.py" --archive "$work/upstream.tar.gz" \
    --original-recipe "$recipe" --target "$work/source" > "$output/source.json"

phase=dependency-index
# APT uses signed Ubuntu Noble index and a private copy of the host inventory.
# Downloaded dependencies are extracted without installing or running package scripts.
apt_root=$work/apt
mkdir -p -- "$apt_root/lists/partial" "$apt_root/archives/partial" "$work/sysroot"
cp -- /var/lib/dpkg/status "$apt_root/status"
keyring=/usr/share/keyrings/ubuntu-archive-keyring.gpg
test -s "$keyring"
printf '%s\n' "deb [arch=arm64 signed-by=$keyring] https://ports.ubuntu.com/ubuntu-ports noble main universe" \
    "deb [arch=arm64 signed-by=$keyring] https://ports.ubuntu.com/ubuntu-ports noble-updates main universe" \
    "deb [arch=arm64 signed-by=$keyring] https://ports.ubuntu.com/ubuntu-ports noble-security main universe" > "$apt_root/sources.list"
cat > "$apt_root/private.conf" <<EOF
Dir::Etc::parts "-";
Dir::Etc::main "-";
Dir::Etc::sourcelist "$apt_root/sources.list";
Dir::Etc::sourceparts "-";
Dir::State::status "$apt_root/status";
Dir::State::lists "$apt_root/lists";
Dir::State::extended_states "$apt_root/extended_states";
Dir::Cache::archives "$apt_root/archives";
Dir::Cache::pkgcache "$apt_root/pkgcache.bin";
Dir::Cache::srcpkgcache "$apt_root/srcpkgcache.bin";
Acquire::Languages "none";
APT::Update::Error-Mode "any";
EOF
export APT_CONFIG=$apt_root/private.conf
apt-get update > "$work/apt-update.log" 2>&1
phase=dependency-download
apt-get --download-only --no-install-recommends -y install \
    ffmpeg libargtable2-dev libavformat-dev libavcodec-dev libavutil-dev libswscale-dev > "$work/apt-download.log" 2>&1
phase=dependency-signatures
for release in "$apt_root"/lists/*_InRelease; do
    gpgv --keyring "$keyring" "$release" >/dev/null 2>&1
    printf '%s  %s\n' "$(sha256sum "$release" | cut -d' ' -f1)" "$(basename -- "$release")"
done | sort > "$output/signed-indexes.sha256"
phase=dependency-extraction
: > "$output/downloaded-packages.tsv"
shopt -s nullglob
for package in "$apt_root"/archives/*.deb; do
    name=$(dpkg-deb -f "$package" Package)
    version=$(dpkg-deb -f "$package" Version)
    architecture=$(dpkg-deb -f "$package" Architecture)
    [[ $architecture == arm64 || $architecture == all ]]
    digest=$(sha256sum "$package" | cut -d' ' -f1)
    apt-cache show "$name=$version" | grep -Fx "SHA256: $digest" >/dev/null
    printf '%s\t%s\t%s\t%s\n' "$name" "$version" "$architecture" "$digest" >> "$output/downloaded-packages.tsv"
    dpkg-deb -x "$package" "$work/sysroot"
done
sort -o "$output/downloaded-packages.tsv" "$output/downloaded-packages.tsv"
phase=dependency-pkg-config
lib=$work/sysroot/usr/lib/aarch64-linux-gnu
extra_lib=$work/sysroot/lib/aarch64-linux-gnu
export LD_LIBRARY_PATH="$lib:$extra_lib"
# APT unpacks libpulse0's private companion below the multiarch directory.
# The system loader configuration on this runner does not include our sysroot.
if [[ -d $lib/pulseaudio ]]; then
    LD_LIBRARY_PATH+=":$lib/pulseaudio"
fi
export PKG_CONFIG_PATH="$lib/pkgconfig:$work/sysroot/usr/share/pkgconfig"
export PKG_CONFIG_SYSROOT_DIR="$work/sysroot"
export LDFLAGS="-Wl,-rpath-link,$lib -Wl,-rpath-link,$extra_lib"
# Ubuntu argtable2 does not reliably ship a .pc file.
export argtable2_CFLAGS="-I$work/sysroot/usr/include"
export argtable2_LIBS="-L$lib -largtable2"
for library in libavformat libavcodec libavutil libswscale; do
    printf '%s=%s\n' "$library" "$(pkg-config --modversion "$library")"
done > "$output/pkg-config-versions"

cd -- "$work/source"
phase=compile-autogen
timeout 90 bash ./autogen.sh > "$work/build.log" 2>&1
phase=compile-configure
timeout 90 ./configure >> "$work/build.log" 2>&1
phase=compile-make
timeout 180 make -j2 >> "$work/build.log" 2>&1
binary=$work/source/comskip
test -x "$binary"

phase=fixture-loader
ffmpeg=$(command -v ffmpeg || true)
ffprobe=$(command -v ffprobe || true)
if [[ -x $work/sysroot/usr/bin/ffmpeg ]]; then
    ffmpeg=$work/sysroot/usr/bin/ffmpeg
    ffprobe=$work/sysroot/usr/bin/ffprobe
fi
test -x "$ffmpeg" && test -x "$ffprobe"
: > "$work/fixture-ldd"
for tool in "$ffmpeg" "$ffprobe"; do
    ldd "$tool" >> "$work/fixture-ldd"
done
awk '$2 == "=>" && $3 == "not" && $4 == "found" && $1 ~ /^[[:alnum:]_.+-]+$/ {print $1}' \
    "$work/fixture-ldd" | sort -u > "$output/fixture-unresolved-sonames.txt"
if grep -Fq 'not found' "$work/fixture-ldd"; then
    exit 127
fi
printf '%s\n' 0 > "$output/fixture-loader-exit"

phase=detector
cd -- "$work"
"$ffmpeg" -hide_banner -v error -f lavfi -i 'testsrc2=size=320x240:rate=25' \
    -f lavfi -i 'sine=frequency=450:sample_rate=44100' -t 8 -threads 1 -c:v mpeg2video -c:a mp2 -f mpegts owned.ts
"$ffprobe" -v error -show_entries stream=codec_name:format=duration -of json owned.ts > "$work/media.json"
sha256sum owned.ts > "$work/before.sha256"
help=0
timeout 15 "$binary" --help > "$work/help.stdout" 2> "$work/help.stderr" || help=$?
test "$help" = 2
grep -Eq '^Comskip 0\.83\.001' "$work/help.stderr"
detector=0
timeout 45 "$binary" --ini="$config" owned.ts > "$work/detector.stdout" 2> "$work/detector.stderr" || detector=$?
test "$detector" = 1 && test -f owned.edl && test ! -s owned.edl
sha256sum --strict -c --quiet "$work/before.sha256"

phase=proof
cmp --silent /var/lib/dpkg/status "$apt_root/status"
printf '%s\n' "$help" "$detector" > "$output/detector-statuses"
python3 -B "$scope/project.py" "$work" "$output" "$binary" "$ffmpeg" > "$output/receipt.json"
phase=complete
