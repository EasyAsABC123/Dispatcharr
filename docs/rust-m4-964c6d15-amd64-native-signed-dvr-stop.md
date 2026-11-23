# M4 ordinary AMD64 Native signed DVR stop

Packaged application `964c6d159057d14048d8bb3a31c0e012e28c763e` passed one Native-only AMD64 Docker Compose smoke using ordinary Selected AIO. The exact package manifest is `4e5c79de482fb54550e9062ca62b9b1e28836ef0b1de58d7389c01be3d1fea8c`. The package preflight, build, verification and outer result all exited 0. The smoke preflight, execution, cleanup, proof and outer result also exited 0.

An authenticated booking returned 201 and the enabled one-off schedule naturally started one recorder with one upstream connection and one provider slot. The fixture fetched the signed HLS playlist and two segments. The segment bodies matched the encoder files byte for byte and passed strict decode with 200 video frames and 376 audio frames. The signed stop returned 200 with status `stopped` before the scheduled end. The persisted row retained `stopped` and successful remux. The partial MKV passed strict decode with 331 H.264 frames and 618 AAC frames, exit 0 and empty decoder stderr.

The observer identified the FFmpeg child and its Main parent by PID and start time in the same boot. Across 122 samples, it first observed the child generation absent 2.438 seconds after the stop request began. The same Main generation was still live. A later owned retirement check found the exact recorder lease, recording-active key, HLS directory, upstream connection and provider slot gone. No stop flag remained for that client. The graceful container stop took 0.425 seconds. Main exited 0 without OOM. Independently verified cleanup found all three recorded container IDs absent, no original owned process generations, and empty project container, network and volume inventories.

The smoke observed one recorder and one upstream. A concurrent ordinary viewer or sibling recording was outside this sample. The observer established process reaping and verified subsequent lease retirement. The first instant of lease disappearance, any explicit application disconnect, and internal stderr-join timing were unobserved. An independent strict paired Source replay and the existing M4 parity and performance gates remain open.

[Machine-readable receipt](../scripts/rust-integration/m4-964c6d15-amd64-native-signed-dvr-stop.json)

Full credit belongs to the original Dispatcharr authors and contributors and the authors of its dependencies.
