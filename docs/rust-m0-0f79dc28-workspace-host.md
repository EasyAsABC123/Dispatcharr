# Exact host workspace checks on 0f79dc28

The reviewed `0f79dc28` application combines the ordinary forced XMLTV handoff fix with formatting in twelve Rust files. The source map protects 497 React and attribution inputs. The exact AMD64 image built successfully from 2,036 reviewed inputs.

Rust 1.90.0 passed `cargo fmt --all -- --check`, strict workspace Clippy, and Cargo workspace tests with locked offline dependencies. Tests completed across 193 binaries with **1,491 passes**, **zero failures**, and **814 ignored**. The run used the fixtures’ disposable Unix sockets and local loopback. The prior formatter-only run under the sandbox stopped with a socket permission error. Its authorized retry passed 1,488 tests with zero failures. All literal exits remain recorded.

These results cover the host workspace and the exact image build. The separate Compose workspace verifier, full migration acceptance and matched performance remain open. The original Dispatcharr authors and contributors retain full credit.

[Machine-readable evidence](../scripts/rust-integration/m0-0f79dc28-workspace-host.json)
