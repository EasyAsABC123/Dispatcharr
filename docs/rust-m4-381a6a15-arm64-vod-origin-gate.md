# ARM64 same-client VOD origin admission

The controlled Lappy Compose attempt pinned Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and the signed ordinary ARM64 package for `381a6a157d1f860527906794bb8c63859fcc8176`. Both preflight engine fences passed. The physical fixture started only its private ARM64 origin and checked the generated MP4 before starting any application or making a media request.

An independent hash of the ARM64 file matched the origin’s own stats. The file was 4,299,461 bytes with SHA-256 `f4a7cbe4e50eaf6cd1578933b8eb8701eb83b384182d6af1f488461e5586dd2f`. FFprobe identified 30.0 seconds of H.264 video and AAC audio, and the origin used FFmpeg 9.0.2-Dispatcharr. The previously qualified AMD64 input was 4,299,455 bytes with SHA-256 `4f3236e89fd6d9fee4f18bb1cd1e9759113a204dc64b74efa7a704fd4c6fc11c`. The ARM64 origin reported the required configured pace of 262,144 bytes per second. Its media GET, HEAD and served-byte counts stayed at zero, so delivery pacing was never exercised.

The exact input check stopped the attempt with physical and outer exit `1`. Original and Native application startup, signed playback, Redis session transitions and media decode remain untested in this attempt. The audit exited `0` and confirmed that the one owned origin container and internal network ID and name were absent, both declared volume names were absent, and the project inventories were empty. No media files left Lappy and the qualified hash was preserved. The raw pair SHA-256 was `cbccc67177fa3fe588d8251383bf44ec78f92393a5a14a756b8a954d7f483a69`.

The precise cause of the encoded difference remains unknown. A physical replay needs a locally available synthetic MP4 matching the existing hash or explicit admission of a separately qualified ARM64 fixture. This attempt leaves ARM64 same-client VOD parity open.

Application and React credit belongs to the original Dispatcharr authors and contributors.
