# Original archive decoder socket diagnostic

The stock Original archive decoder exited zero and the fixture
retired cleanly. The archive session again failed the five-second
retirement gate. The read-only FFmpeg observer retained ten samples,
but it found more than one matching decoder socket. The fixture
could not associate the decoder's final request with a unique
backend connection.

The result leaves the Original WSGI close, generator unwind, and
upstream provider close cause unresolved. No comparative performance
measurement ran. Original Dispatcharr and its upstream contributors
retain full credit.

[Machine-readable decoder scope](../scripts/rust-integration/m4-d41abbb9-original-decoder-socket-scope.json)
