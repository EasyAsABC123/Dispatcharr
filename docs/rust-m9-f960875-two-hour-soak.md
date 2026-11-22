# Two-hour shared-playback soak on f960875

Original Dispatcharr authors and contributors retain full application and React credit. See [upstream attribution](../ATTRIBUTION.md).

The exact published Rust image passed the 120-second, 360-second, and 7,200-second phases in three isolated Compose projects. Ten viewers in the full phase shared one upstream connection, one provider slot, and one H.264/AAC encoder. All 12 scheduled XMLTV imports reached their required terminal work during the active phase. The two-hour run passed 140 audio/video clip decodes and retired the encoder, viewer ownership, and all five owned Compose roles.

The post-idle median working-set change was -92.44 MiB against the existing 64 MiB ceiling. Measured maximum count changes were -2 processes, -3 threads, -211 file descriptors, and -56 socket descriptors. Resource budgets, the runner, and workload remained fixed across the same-image phase handoffs. The full controller and independent verifier both recorded literal exit 0.

This evidence verifies the owned two-hour workload. The release matrix's workflow-parity dependency and comparative performance gate remain open. [Machine-readable evidence](../scripts/rust-integration/m9-f960875-two-hour-soak.json)
