# M9 current-image two-hour shared-playback soak

The exact ordinary AMD64 Rust application `0c5133b` passed the two-minute,
six-minute, and two-hour phases in separate owned Docker Compose projects.
During the full phase, ten viewers shared one upstream connection, provider
slot, and H.264/AAC encoder. The run sustained 7,200 seconds of playback,
completed all 12 scheduled XMLTV refreshes, and passed 140 audio/video
decodes. It recorded 1,441 active progress samples and verified the
ordinary refresh-token exchange during playback.

The median post-idle working set increased by 22.33 MiB against the 64 MiB
limit. The resource proof recorded changes of −2 processes, −3 threads,
−211 file descriptors, and −56 socket descriptors. The controller and
independent proof exited 0. The fixture retired the encoder and all five
owned service generations and verified empty project inventories.

The scoped two-hour gate passed. The integrated release matrix and paired
release performance remain open. Original Dispatcharr, its React frontend,
and its dependencies retain their existing attribution and licenses.

[Machine-readable receipt](../scripts/rust-integration/m9-0c5133b-two-hour-soak.json)
