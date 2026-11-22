# Three concurrent M9 Compose fixtures on f960875

The original Dispatcharr authors and contributors retain full credit for the application. See [Attribution](../ATTRIBUTION.md).

On November 22, 2026, Beepboop ran three existing release-evidence rows through one coordinator lease on AMD64. The run used the ordinary Rust `f960875f26d26bf1108a299530064d24ecc462a3` package. The three fixtures each passed their functional evaluator and verified exact cleanup.

| Required row | Reserved host ports |
| --- | --- |
| `package/current-packaged` | 61000–61999 |
| `package/output-profile` | 62000–62999 |
| `package/user-agent` | 63000–63999 |

The live observer inspected all three distinct projects in 40 samples spanning 25.68 seconds. It recorded 14 of the 15 configured host-port bindings, each in its assigned range. The Current fixture used its candidate endpoint for readiness and 64 HTTP cases. The observer listed the candidate container running, then inspected it after it stopped and missed the `127.0.0.1:61541` binding for container port `9192`.

The coordinator returned its partial-run exit 2. Cleanup returned 0. The strict overlap reducer and overall owner each returned 1 because the run lacked one live port observation. This experiment repeated three previously covered rows, added zero distinct required rows, and collected no comparative performance measurement. The [machine-readable receipt](../scripts/rust-integration/m9-f960875-parallel-compose.json) retains all literal exits and distinguishes observed mappings from the missing observation.
