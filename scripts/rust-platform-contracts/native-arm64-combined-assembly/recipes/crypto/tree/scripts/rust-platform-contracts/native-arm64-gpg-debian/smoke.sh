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
export HOME="$work/home"
parser_code=0
/usr/local/bin/gpg --no-options --homedir "$work/home" --batch --no-tty --no-autostart --no-keyring \
    --no-random-seed-file --list-packets "$packet" > "$work/packet.txt" 2> "$work/parser.stderr" || parser_code=$?
printf 'exact-gpg-command-exit=%s\n' "$parser_code" >&2
if [ "$parser_code" != 0 ] || [ -s "$work/parser.stderr" ]; then
    kind=other
    if grep -Eqi 'invalid option|unknown option' "$work/parser.stderr"; then
        kind=option
    elif grep -Eqi 'Permission denied|not permitted|Read-only file system' "$work/parser.stderr"; then
        kind=filesystem-access
    elif grep -Eqi 'gpg-agent|keyboxd|trustdb' "$work/parser.stderr"; then
        kind=local-helper-or-trustdb
    elif grep -Eqi 'packet|processing message failed|invalid data' "$work/parser.stderr"; then
        kind=packet-processing
    elif [ ! -s "$work/parser.stderr" ]; then
        kind=no-diagnostic
    fi
    printf 'gpg-parser-diagnostic=%s\n' "$kind" >&2
    exit 1
fi
if [ -n "$(find "$work/home" -mindepth 1 -print -quit)" ]; then
    printf 'synthetic-home=nonempty\n' >&2
    exit 1
fi
printf 'synthetic-home=empty\n' >&2
if [ "$(sha256sum "$packet" | cut -f1 -d' ')" != "$(cat "$work/packet.sha256")" ]; then
    printf 'packet-bytes=changed\n' >&2
    exit 1
fi
printf 'packet-bytes=unchanged\n' >&2
emit_sha() {
    label=$1
    input=$2
    if [ ! -f "$input" ]; then
        printf 'file-read-%s=missing-or-not-regular\n' "$label" >&2
        return 1
    fi
    if [ ! -r "$input" ]; then
        printf 'file-read-%s=unreadable\n' "$label" >&2
        return 1
    fi
    if ! sha256sum "$input" 2> "$work/hash.stderr"; then
        printf 'file-read-%s=read-error\n' "$label" >&2
        return 1
    fi
}
printf 'BEGIN files\n'
if ! find "$prefix" "$docs/crypto-copyright" -type f > "$work/paths" 2> "$work/find.stderr"; then
    printf 'collector-tree-traversal=error\n' >&2
    exit 1
fi
LC_ALL=C sort "$work/paths" > "$work/sorted"
while IFS= read -r input; do
    case "$input" in
        "$prefix"/*) emit_sha private-closure "$input" ;;
        "$docs"/crypto-copyright/*) emit_sha installed-notice "$input" ;;
        *) printf 'collector-tree-traversal=unexpected-path\n' >&2; exit 1 ;;
    esac
done < "$work/sorted"
printf 'collector-tree-traversal=complete\n' >&2
emit_sha gpg-wrapper /usr/local/bin/gpg
emit_sha original-license "$docs/LICENSE"
emit_sha original-attribution "$docs/ATTRIBUTION.md"
printf 'fixed-read-and-hash=complete\n' >&2
printf 'END files\nBEGIN manifest\n'
if [ ! -r "$docs/crypto-runtime.json" ] || ! cat "$docs/crypto-runtime.json" 2> "$work/manifest.stderr"; then
    printf 'file-read-crypto-manifest=read-error\n' >&2
    exit 1
fi
printf 'manifest-read=complete\n' >&2
printf 'END manifest\nBEGIN loader\n'
cat "$work/loader.txt"
printf 'END loader\nBEGIN version\n'
head -n 1 "$work/version.txt"
printf 'END version\nBEGIN packet\n'
cat "$work/packet.txt"
printf 'END packet\nBEGIN packet-digest\n'
cat "$work/packet.sha256"
printf 'END packet-digest\n'
