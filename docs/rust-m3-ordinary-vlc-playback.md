# Ordinary Main VLC playback

The original Dispatcharr authors and contributors retain full application
and algorithm credit. VLC, FFmpeg and other dependency authors retain
credit for their work.

The exact `c74ea312bdc7` package passed the Compose VLC playback check with
ordinary Native Main. Its ready response had no selected worker registry.
Two authenticated readers each received 1,279,340 bytes of MPEG-TS over
HTTP 200 and decoded 74 video frames with audio and zero decoder errors.
They shared one VLC input process, one origin connection and one provider
slot. After the readers closed, the idle input and upstream retired. Main
and the owned VLC proxy exited 0. The controller exited 0 with both EOFs,
and teardown confirmed all three owned containers, their original host
processes, and the project network and volumes were absent.

The retained Original25 VLC leaf also produced two clean A/V decodes from
one VLC process, origin connection and provider slot. Original teardown
observed upstream retirement after its application roles stopped.
Original25's parent controller exited 1 with exact owned cleanup and a
separate failed Redirect leaf. The VLC leaf passed and was reused.

The first Native attempt passed the media checks, stopped Main, and
confirmed exact cleanup. Its controller exited 1 because the inherited
startup observer stayed attached during playback and hit its 16 MiB
journal limit. The retry omitted that observer only for this playback
context and required the real VLC proxy to be the sole fixture role.
The VLC checks and strict teardown assertions remained in force.

This result covers equivalent authenticated viewers on a single VLC profile
under ordinary Main. Per-format performance and full M3 acceptance remain open.

[Redacted playback receipt](../scripts/rust-integration/milestone-batch113/ordinary-vlc-playback.json)
