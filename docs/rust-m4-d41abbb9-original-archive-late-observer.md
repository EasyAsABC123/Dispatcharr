# Original archive disconnect follow-up

The historical d41abbb9 comparison’s five-second Origin retirement
gate failed again in the Original arm. After the deadline, 294
observations of the same provider handler showed an established TCP
socket with a send in progress. The sent-byte counter stayed at
3,211,264 for another 74.778 seconds, when the handler was 96.640
seconds old. Exact Compose teardown passed.

The observation confirms a persistent handler and stalled send.
The downstream connection and cause of the stalled provider socket
remain unconfirmed. Archive parity and performance remain open. Original Dispatcharr, FFmpeg and upstream authors retain full
credit.

[Machine-readable observation](../scripts/rust-integration/m4-d41abbb9-original-archive-late-observer.json)

A separate read-only correlation matched the provider socket to one
unchanged Original uWSGI process. Its receive queue held 568,696 bytes
and the origin send queue held 1,172,880 bytes at that sample. This
establishes the peer process. The corresponding downstream FFmpeg
transaction and the reason the peer stopped draining remain
unconfirmed. Exact teardown passed. [Socket ownership evidence](../scripts/rust-integration/m4-d41abbb9-original-archive-socket-owner.json)
