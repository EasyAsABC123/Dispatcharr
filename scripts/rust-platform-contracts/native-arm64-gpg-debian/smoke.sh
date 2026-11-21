#!/bin/sh
set -eu
export LC_ALL=C TZ=UTC PATH=/usr/local/bin:/usr/bin:/bin
unset LD_LIBRARY_PATH LD_PRELOAD
prefix=/opt/dispatcharr-crypto
docs=/usr/share/doc/dispatcharr
loader="$prefix/lib/ld-linux-aarch64.so.1"
[ "$(id -u)" = 65532 ]
[ "$(uname -m)" = aarch64 ]
[ "$(dpkg --print-architecture)" = arm64 ]
[ ! -e /dispatcharrpy ] && [ ! -e /usr/bin/gpg ]
test -f "$docs/LICENSE" && test -f "$docs/ATTRIBUTION.md"
work=$(mktemp -d /tmp/owned-gpg.XXXXXXXX)
trap 'rm -rf -- "$work"' EXIT
mkdir -m 0700 "$work/home"
packet="$work/owned.pgp"
printf '\313\040b\000\000\000\000\000Dispatcharr owned literal\n' > "$packet"
sha256sum "$packet" | cut -f1 -d' ' > "$work/packet.sha256"
"$loader" --inhibit-cache --library-path "$prefix/lib" --list "$prefix/bin/gpg" > "$work/loader.txt"
/usr/local/bin/gpg --version > "$work/version.txt"
printf 'loader-and-version-completed\n' >&2
/usr/local/bin/gpg --no-options --homedir "$work/home" --batch --no-tty --no-autostart --no-keyring \
    --list-packets "$packet" > "$work/packet.txt"
test -z "$(find "$work/home" -mindepth 1 -print -quit)"
test "$(sha256sum "$packet" | cut -f1 -d' ')" = "$(cat "$work/packet.sha256")"
printf 'BEGIN files\n'
find "$prefix" "$docs/crypto-copyright" -type f | LC_ALL=C sort | while IFS= read -r path; do
    sha256sum "$path"
done
sha256sum /usr/local/bin/gpg "$docs/LICENSE" "$docs/ATTRIBUTION.md"
printf 'END files\nBEGIN manifest\n'
cat "$docs/crypto-runtime.json"
printf 'END manifest\nBEGIN loader\n'
cat "$work/loader.txt"
printf 'END loader\nBEGIN version\n'
head -n 1 "$work/version.txt"
printf 'END version\nBEGIN packet\n'
cat "$work/packet.txt"
printf 'END packet\nBEGIN packet-digest\n'
cat "$work/packet.sha256"
printf 'END packet-digest\n'
