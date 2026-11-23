# 1b490f5d Rust workspace and AMD64 package

The exact candidate is `1b490f5d964dc4df34f27837e110c6eb755f79cf`. It adds DVR series preview and bulk removal, aligns Notification Center JSON order and HTTP headers, and matches backup inventory timestamp rounding and read headers.

The locked workspace passed formatting, strict Clippy across all targets, and 1,731 enabled tests in 193 test result groups. The runner recorded zero failures and 821 ignored tests. Those ignored cases remain separate from the enabled test result. A local loopback preflight passed. The source custody check covered all Rust inputs and five tracked test fixtures outside Rust.

The ordinary AMD64 Docker build and image verification exited zero. The application archive contains 2,181 files, including 497 protected React, attribution and license inputs. The build produced image `sha256:76a95726884e7c5f8f49454fdf44dc0d8ce340d541e9be3365ae60ec04a2afd4` and build manifest SHA-256 `147d784d7747d35d0210abf8f9733139cd78cab865e15fde905ff13bcada3667`. The archived application tree digest is `ccdcbf46befb54c51639e92a0949d3cb02e58de36aa5f311adbb23409bd32826`.

The package is the input to the paired AMD64 Compose tests. The build and workspace result establishes qualification for those scoped tests. ARM64 runtime, comparable performance, and full milestone acceptance have their own gates.

Original Dispatcharr authors and contributors retain full project credit. The candidate preserves the attribution, license and React files, including their Git modes.
