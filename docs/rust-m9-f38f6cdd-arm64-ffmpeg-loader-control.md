# F38 ARM64 loader and FFmpeg startup comparison

Two separate disposable fixtures used the same audited ordinary F38 ARM64 binary, package, finite MPEG-TS prefix, Origin pacing and signed viewer. One ran the packaged dynamic loader’s `--list` operation. The other ran the exact packaged FFmpeg with `-version`. Each command finished before the signed media request while Origin and provider state remained idle. Both fixtures and their independent auditors exited zero, and exact cleanup left the engine empty.

| Candidate file charge | Loader `--list` | FFmpeg `-version` |
| --- | ---: | ---: |
| Before pre-media command | 110,743,552 B | 110,743,552 B |
| After command | 159,461,376 B | 180,506,624 B |
| Increase before media | **48,717,824 B** | **69,763,072 B** |
| First useful A/V payload | 182,530,048 B | 182,530,048 B |
| After exact drain | 182,530,048 B | 182,530,048 B |

The loader-only bracket added 47,759,360 B of active file charge. Candidate anonymous and mapped-file counters were flat. Full startup added 68,841,472 B of active file charge and 4,096 B of anonymous memory. Baseline and Origin file counters were flat across both pre-media brackets. The 21,045,248 B difference describes two separate runs. It does not identify the pages charged during either run or prove an isolated FFmpeg initialization cost. [Version-only control](rust-m9-f38f6cdd-arm64-ffmpeg-startup-file-charge.md)

The exact loader resolved all 55 sealed SONAMEs from the relocated package. Its public proof excludes addresses and private execution paths. The first attempt keeps its literal exit 1 and exact cleanup. The retained command itself exited zero. A file-only replay verified its original SHA-256 and corrected parser behavior before the passing retry. [Machine-readable paired controls](rust-m9-f38f6cdd-arm64-ffmpeg-loader-control.json)

The ARM64 recipe retains the exact Original custom FFmpeg and FFprobe ELF bytes and uses one identical 55-entry dependency inventory for both tools. Every entry is observed in the loader result. The evidence does not support removing a dependency, switching FFmpeg builds, narrowing codec features, or changing the formal working-set calculation. [Native media assembly recipe](../scripts/rust-platform-contracts/native-arm64-combined-assembly/recipes/ffmpeg-vlc/tree/scripts/rust-platform-contracts/native-arm64-vlc-debian/ffmpeg/assemble.py)

The next benchmark can compare the exact Original ARM64 media launcher and the relocated Rust package on Lappy using the same media binary hash, custom argv, and fixture. Record starting cache state, run starts in both orders, and bracket startup, first useful payload, and idle after exact retirement. A bounded read-only sample of the single live encoder’s executable and dependency mappings can report resident and proportional-set bytes for fixed approved file labels where the guest permits it. Preserve active/inactive file and anonymous cgroup counters alongside those process measurements. The benchmark would test whether the package layout changes startup cost independently of viewer work and show which mapped objects dominate. It would not provide file-level attribution for pages retained after the encoder exits.

The same binary’s formal S120 remains failed at the unchanged 64 MiB working-set gate. S360, S7200, full M9 and release-performance qualification remain open. [Formal S120 evidence](rust-m9-f38f6cdd-arm64-s120-idle-failure.md)

Original Dispatcharr, FFmpeg and dependency authors retain full credit.
