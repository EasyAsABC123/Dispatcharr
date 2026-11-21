# Standalone VLC wrappers on native ARM64 Debian

A native ARM64 runner assembled the pinned Original VLC files and the
previously verified FFmpeg tools into a temporary Debian Bookworm-slim
image. The network-disabled, read-only guest ran as UID and GID 65532,
checked both VLC wrapper versions, and used `cvlc` to remux a generated
local MPEG-TS input. FFmpeg decoded both output streams and the input
hash stayed unchanged. The output was 747,676 bytes and 7.810922 seconds,
with 195 MPEG-2 video frames at 320 × 240 and 298 MP2 audio frames.
The controller independently checked 792 VLC file hashes. Build, guest
and controller each exited 0. The exact temporary container and image
were removed. [Successful GitHub Actions run](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44873182717/job/136752036449)

The first run built and completed the wrapper guest, with exact cleanup.
Its controller exited 1 after verifying the 792 hashes because it
expected a different VLC version prefix on stdout. The retry changed
that verifier assertion and retained the guest workload.
[First run](https://github.com/EasyAsABC123/Dispatcharr/actions/runs/44872878694)

The Original VLC probe’s diverted loader notice gap remains recorded.
The standalone build checked the installed `libc6:arm64` notice hash
from independent same-donor evidence. The FFmpeg and ffprobe installed
tool notice gaps also remain. This evidence covers the standalone local
remux and decode. Full ARM64 application qualification and image
publication remain open. [VLC donor receipt](rust-native-arm64-vlc-donor.md) · [Loader notice correlation](../scripts/rust-integration/m10-native-arm64-vlc-loader-notice-correlation.json) · [Standalone wrapper receipt](../scripts/rust-integration/m10-native-arm64-vlc-debian-wrapper.json)

Original Dispatcharr, VideoLAN, FFmpeg, Ubuntu, Debian and dependency
authors retain full credit.
