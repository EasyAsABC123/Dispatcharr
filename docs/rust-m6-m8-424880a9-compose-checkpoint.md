# Copied-data reads, dismissal and provider rehash on 424880a9

The ordinary and copied SQLite AMD64 images were verified from exact
application source `424880a9ba0a375eab8f985b1a279252c4d0a54b`. Both strict host lint configurations
passed. The ordinary workspace passed 1,574 tests
and the copied-feature check passed 1,133 tests.

The copied SQLite image matched 34 Streams GET and HEAD observations for
account filtering and combined Name and account queries. Those checks
covered Page, IDs, and filter options. The Logo usage-filter comparison
matched 16 GET and HEAD observations before restart and 16 after restart.
The same signed subjects retained access and the scoped SQL stayed stable.
The notification cohort matched eight dismiss-all requests and four new
personal dismissal rows. Signed actor results and SQL state survived
restart. Each cohort verified exact cleanup. Two bulk observer attempts
exited 1 after the guest completed, first at the inherited single-dismiss
checkpoint and then at the inherited final log check. The corrected
same-image replay passed both checks and the independent reducer.

The ordinary provider rehash, using the existing opt-in, returned HTTP 200
on both backends. Each worker result confirmed the two-stream
merge and ordered update events, with an empty queue and a released lock.

The observed reply-loss case first returned HTTP 503 on Rust and HTTP
200 on Original. Rust committed 1 queue frame and Original
committed two frames for the same task identity. The repeats were HTTP 200
without another committed frame. Both matched the scoped SQL result and
the success terminal. First-response parity and frame-count parity are
recorded independently. This observation covers the tested same-process
owner arrangement.

The formal milestone matrix, copied notification cross-writer HTTP parity,
and comparative application performance remain open.
[Machine-readable evidence](../scripts/rust-integration/m6-m8-424880a9-compose-checkpoint.json)

Original Dispatcharr and dependency authors retain full credit for the
application, models, algorithms, and protocols.
