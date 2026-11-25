# Dispatcharr signed Live MPEG-TS quiet comparison

Exact Rust candidate `d6be0944e85d6cdbc5ef9c2af06e7e87aac030f8` and Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.
AMD64 image `sha256:d16b57372da085ca48c3040bdd7adebca98972bb0251241b7dd6f7762bbb692d` and ordinary manifest SHA-256 `d0ce18eb13586cc688ef74ea54c13d961f59d590af2675bc4e46246154915de0`.
Released evidence tip `4ab97f9e8bb228ecd569357184ce593122243e15`, protocol SHA-256 `ffb54395d3d6e44e5ed4ee0053db48a31091d014f7da778a59a16bade21a50a1`, and private raw report SHA-256 `aa20d8a094438a0d5d2f77ee6ae1974f5a2f204b71e5cb83eb2b6ff3e4f6fb47`.

Six signed functional windows and three fresh Original qualifiers passed. Nine matched trials passed all fixed gates and exact cleanup across 18 measured arms.
The comparison covers signed continuous MPEG-TS with the active HTTP/WebSocket backend, PostgreSQL, Redis and a deterministic origin.

| Viewers | CPU change | Peak working memory change | First byte p95, Original → Rust (ms) | First byte change (50 ms floor) | Throughput, Original → Rust (bytes/s) | Throughput change |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | -35.15% | -50.02% | 20.76 → 4.29 | +0.00% | 247339 → 247339 | +0.00% |
| 10 | -58.35% | -63.03% | 136.09 → 17.56 | -63.26% | 2494713 → 2473391 | -0.85% |
| 100 | -87.23% | -58.70% | 1322.92 → 76.34 | -94.23% | 25151824 → 24738762 | -1.64% |

Changes are medians of three within-pair Rust-to-Original ratios for each viewer count. Working memory is the peak simultaneous summed working set in each arm.

Full M1 acceptance and release acceptance remain open.

Original Dispatcharr and dependency authors retain full authorship and license credit.
