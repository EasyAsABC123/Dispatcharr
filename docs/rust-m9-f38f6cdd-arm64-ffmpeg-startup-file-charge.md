# F38 ARM64 file charge during FFmpeg startup

The exact audited `f38f6cdd09ad214d3d3ff18b0a46c286637fc9d1` ARM64 candidate ran the packaged FFmpeg executable once with `-version` before the first signed media request. The command used the existing ARM64 loader and relocated library path. The Origin, provider and client observations stayed idle across the command. The normal signed viewer then ran and drained. The campaign, independent proof and exact teardown all exited zero. The engine was empty afterward.

| Candidate counter | Before FFmpeg startup | After version command exited | Change |
| --- | ---: | ---: | ---: |
| File charge | 110,743,552 B | 180,506,624 B | +69,763,072 B |
| Active file | 78,327,808 B | 147,169,280 B | +68,841,472 B |
| Anonymous | 15,892,480 B | 15,896,576 B | +4,096 B |
| File mapped | 46,231,552 B | 46,231,552 B | 0 B |

Baseline and Origin file charges stayed flat across the startup command. Candidate file charge rose another 2,023,424 B by the first useful A/V payload and remained at 182,530,048 B through drain. Earlier same-binary runs observed comparable candidate file-charge increases at first payload (+71,610,368 B) and in the failed S120 idle medians (+71,622,656 B). The controls began with different file-charge baselines. The startup-only run demonstrates that substantial candidate file charge can appear before streaming and without a matching Origin charge reduction. It does not establish the underlying pages, a transfer of charges, or which specific dependency dominates. [Earlier one-viewer witness](rust-m9-f38f6cdd-arm64-one-viewer-file-charge.md) · [Formal S120 failure](rust-m9-f38f6cdd-arm64-s120-idle-failure.md)

The cache syscall returned `EPERM` for both binaries after a successful read-only open, and neither fd owner matched the reader. Origin returned `EACCES` at the cross-root open. These failures left those files’ resident-page counts unobserved. The kernel checks ownership, capability or write permission before reporting file residency. [Linux cache syscall implementation](https://kernel.googlesource.com/pub/scm/linux/kernel/git/stable/linux-stable.git/%2B/refs/tags/v6.14.7/mm/filemap.c#4455)

The native ARM64 media recipe preserves the Original FFmpeg and FFprobe ELF bytes and the same 55-entry dependency roster for both tools. The isolated wrapper runs the packaged loader with the relocated runtime library path. This experiment invoked that loader directly. Removing the shell wrapper or deleting FFprobe-only libraries has no demonstrated path to the observed startup reduction. The supported package has no FFprobe-only entries. [Frozen ARM64 media assembly](../scripts/rust-platform-contracts/native-arm64-combined-assembly/recipes/ffmpeg-vlc/tree/scripts/rust-platform-contracts/native-arm64-vlc-debian/ffmpeg/assemble.py)

The next narrower discriminator can measure the same loader’s dependency-list operation in a fresh identical disposable fixture before FFmpeg code runs. Compare its paired cgroup deltas with the version-only result, retain sampled cache state, and verify that every resolved entry matches the sealed roster. The loader list describes resolved dependencies. It does not measure per-inode cgroup charge. [Linux dynamic loader reference](https://www.kernel.org/pub/linux/docs/man-pages/book/man-pages-6.17.pdf)

The original 64 MiB S120 gate remains in force. This startup diagnostic does not qualify S360, S7200, full M9 or release performance. [Machine-readable evidence](rust-m9-f38f6cdd-arm64-ffmpeg-startup-file-charge.json)

Original Dispatcharr, FFmpeg and dependency authors retain full credit.
