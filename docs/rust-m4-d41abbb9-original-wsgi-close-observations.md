# Original archive WSGI close observations

An event-only diagnostic in a disposable Original copy observed
three HTTP 206 requests for the same decoder capability. The two
archive requests and one end-of-file probe all recorded provider
close, generator finalization, iterator close and WSGI response
close. The last archive request reached its response close about
5.78 seconds after that request entered WSGI.

The fixture failed the existing five-second retirement gate and
passed exact teardown. The diagnostic could not tie a particular
downstream decoder socket to any request or to the still-observed
upstream connection. The cause of the missed deadline remains open.
This was a diagnostic, with no performance measurement.

Original Dispatcharr and its upstream contributors retain full credit.
[Machine-readable event observations](../scripts/rust-integration/m4-d41abbb9-original-wsgi-close-observations.json)
