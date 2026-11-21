# Daily version check when the update origin returns HTTP 503

The pinned Original and packaged Rust `5d9e558b70270e2dcc2b478d5ba96bca6a9abb8f` each ran one ordinary
daily version check in Docker Compose. Each sent one request to the owned
test origin, received HTTP 503 and returned `SUCCESS/null`. The durable
schedule advanced once. The worker-ready cache deadline stayed fixed, and the
test observed no version notification row or event. Both projects passed exact
cleanup.

The application manifest was `70de2bcb2205bd4e0c181328b39c3bd1cf5e042f37a3afc4b1bed38af4512b8f`.
Preflight, Compose, and independent reduction exited 0. The redacted proof
SHA-256 is `265266ea2b44231de2b4de20b178aba3a7c50a0a53934a811ffa391183d2fbb7`. This exercise covers the scheduled 503 case.
Full M6 acceptance and comparative performance remain open.
[Redacted receipt](../scripts/rust-integration/m6-5d9e558b-version503.json)

The original Dispatcharr, Django, Celery and django-celery-beat authors and
contributors retain full credit.
