# Unified and static Chrome on 424880a9

Two required browser workflows passed on the same verified ordinary AMD64
image for `424880a9ba0a375eab8f985b1a279252c4d0a54b`. Both used the unchanged
React UI, and each verified 112 shipped React assets.

- Unified HTML verified the paired real browser workflow, 56 HTML GET and
  HEAD observations, and 188 Unified HTTP cases. The owned fixture returned
  to its original state.
- Static Chrome verified 32 browser Accept HTML comparisons and 68 static
  asset comparisons, including 11 required styles and font assets served
  by Rust Main. The temporary asset donor retired.

Both executions passed independent semantic evaluation and cleanup. The
isolated coordinator returned 2 for each scope. These two passes belong
to this exact image. The full formal matrix and comparative application
performance remain open.

[Machine-readable browser evidence](../scripts/rust-integration/m9-424880a9-browser-checkpoint.json)

Original Dispatcharr and dependency authors retain full credit for the
application, models, algorithms, and protocols.
