# Coordinator idle CPU correction

The migration test coordinator now avoids scanning every host process while its child leader is healthy and running. Actual exit, timeout, termination, final and finally paths still scan the real process group; pipe polling and signal/settlement deadlines are unchanged. Unobserved membership uses `None`, never a fabricated empty-group receipt. Application code is unchanged. Original application and tooling authors retain credit.

The [actual host controls](../scripts/rust-integration/coordinator-idle-scan-1/summary.json) used the same PID namespace with 645 numeric `/proc` entries. For an identical 5-second idle child, observer CPU fell from 1.168671 to 0.047808 seconds (95.91% in this single pair); wall times were 5.0556 and 5.0570 seconds. This measures the testing tool, not Dispatcharr performance, and does not establish an application gain.

All 14 before/after lifecycle controls passed their expected assertions: idle, orphaned descendants, zombies, ignored TERM, timeout, output limits and escaped inherited pipes. Escaped-pipe receipts deliberately remain failed/unsettled with both EOF flags false; the test then removes only the recorded owned escaped PID/generation. Other cleanup controls retain actual exit/failure, EOF, group settlement and reaping evidence. Root also observed the complete existing coordinator suite passing 26 tests in 7.390 seconds. Its [tool receipt](../scripts/rust-integration/coordinator-idle-scan-1/Root-full-tests-tool-receipt.json) is labeled separately from raw redirected output.

The public packet contains immutable before/after source snapshots, the exact tested control script, raw safe stdout/stderr and hashes. From the repository in the host PID namespace, with no competing heavy test lease, reproduce using a fresh output directory:

```sh
exec 9> scripts/rust-milestone-batches/.dispatcher.lock
flock -n 9 || exit 75
python3 -B scripts/rust-integration/coordinator-idle-scan-1/check-controls.py --output /absolute/path/to/fresh-controls --namespace-anchor-pid "$$"
python3 -B scripts/rust-milestone-batches/test_coordinator.py
```

Reserve 90 seconds for the controls. The active shell provides the live namespace anchor. The reproduction script is byte-identical to the executed version; configuration points to the packet's verified original snapshot so later canonical changes cannot masquerade as the baseline. The [packet index](../scripts/rust-integration/coordinator-idle-scan-1/index.json) hashes every artifact. No frozen application benchmark, cadence, budget or failed application result is rewritten by this correction.
