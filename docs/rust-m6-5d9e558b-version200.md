# Scheduled version checks with HTTP 200

The pinned Original and Rust application `5d9e558b70270e2dcc2b478d5ba96bca6a9abb8f`
each ran four ordinary daily version-check cases in Docker Compose. The
owned origin returned HTTP 200 for no update, an available update, a repeated
response, and a malformed response. Each occurrence advanced the saved
schedule once and settled as SUCCESS with a null result. The expected
version notice, sequence, and event effects matched for all four cases.
Both projects passed exact teardown.

Full Rust Main emitted one unrelated startup notice before the first task.
The completed replay verified that startup effect separately and checked
that the unrelated state stayed fixed during the scheduled cases.
The first wrapper attempt stopped before Docker due to an existing output
directory. The second reached both backends and exposed the startup observer
mismatch, with exact cleanup. The fresh preflight, Compose run, and
independent reducer each exited 0.

The package manifest is `70de2bcb2205bd4e0c181328b39c3bd1cf5e042f37a3afc4b1bed38af4512b8f`.
The proof SHA-256 is `da6729d170fd0237b5fb7fa207b4ae5a3e32d22434a142b2fddc0dcfc7f10042`. Full M6 and comparative performance remain
open. [Redacted receipt](../scripts/rust-integration/m6-5d9e558b-version200.json)

Original Dispatcharr, Django, Celery and django-celery-beat authors and
contributors retain full credit.
