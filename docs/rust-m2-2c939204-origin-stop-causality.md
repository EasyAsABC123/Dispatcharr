# Origin socket observations during authenticated Stop Channel

Three independent paired Compose trials used exact Rust application `2c939204e070954e7de86b662f362237b5f7f9ae` and the same pinned Original fixture. Both backends kept one second-channel request pending behind an occupied provider slot. The administrator stopped the first channel through the ordinary authenticated route. That same waiting request produced decodable audio and video on each backend in all three trials.

In each trial, the origin recorded a peak of one active handler on either backend, and the old handler exited before the second handler entered. The intervals from observed old-handler exit to second-handler entry were 87.864–93.039 ms for Original and 19.480–66.153 ms for Native. These are fixture observations. The passive observer recorded accepted socket generations, the last successful write, peer receive EOF or reset, and write failure or handler exit. All five owned roles and their process generations retired in each trial.

The earlier uninstrumented repeat remains a literal physical failure. It measured peak one on Original and peak two on Native. The three instrumented trials did not reproduce that peak, so the socket state and causal order of the earlier overlap remain unknown. [Earlier capacity evidence](rust-m2-provider-capacity-retry.md) · [Redacted trial receipt](../scripts/rust-integration/m2-2c939204-origin-stop-causality.json)

The original Dispatcharr and framework authors retain full credit.
