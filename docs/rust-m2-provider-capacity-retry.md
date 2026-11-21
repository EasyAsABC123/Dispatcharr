# Live MPEG-TS provider capacity and waiting

The scoped e9 comparison verified two viewers shared one upstream and that a
normal last-viewer disconnect freed the provider slot for another channel. Both
backends returned HTTP 503 for a second channel while the slot was held.
Original reported the time spent waiting. Rust omitted the field and returned
immediately in that package.

On `2c939204e070954e7de86b662f362237b5f7f9ae`, the same ordinary Rust route retries a full provider
slot before opening input. The Compose comparisons captured three cases.

- With the slot held, both backends returned HTTP 503 and the Original JSON
  field order and spacing. The captured waits were
  2 seconds on Original and
  2 seconds on Rust. Each value
  matched its own measured wait window.
- One authenticated global-stop run delivered audio and video on the same
  pending request on both backends. The raw controller passed and an inherited
  timing assertion failed during reduction. A corrected repeat recorded a
  peak of two Origin handlers on Rust and one on Original. The socket timing
  and handoff parity are still under investigation.
- After an ordinary last-viewer close, the pending request returned HTTP
  503 on Original and HTTP 503 on Rust. Both arms verified
  eventual second-channel playback and peak upstream concurrency of one.

Each pair verified its owned process and container retirement. The workspace
passed formatting, strict Clippy, and 1,558 tests. Other lifecycle faults
and comparative performance remain open. [Exact scoped receipts](../scripts/rust-integration/m2-provider-capacity-retry.json)

The original Dispatcharr and dependency authors retain full credit.
