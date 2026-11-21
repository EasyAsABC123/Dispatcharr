# Scheduled account warning and expiration

The pinned Original and Rust application `5d9e558b70270e2dcc2b478d5ba96bca6a9abb8f` each
ran the saved daily account-expiration task in Docker Compose. Each scheduler
published one task to the default queue. The worker consumed the same
task identity, returned SUCCESS with a null result, and drained the queue.

The fixture gave one active profile an upcoming expiration and another an
expired date. Both runtimes updated the existing warning in place, created
the expiration notice, and removed two obsolete warning rows. The event
receivers saw both notice updates and both dismissals. The inactive
account stayed outside the task, and the provider profiles and unrelated
sentinel notification stayed unchanged.

Both runs retired exactly. The Original project retired two owned
containers and the Selected Jobs and Beat project retired three.
The preflight, Compose controller, and proof reducer all exited 0.

The package manifest is `70de2bcb2205bd4e0c181328b39c3bd1cf5e042f37a3afc4b1bed38af4512b8f`.
The proof SHA-256 is `bd7be21794d2251d3c826a8bfa1fb1a31a30a4ff9506f76e24914f9d0e4858ec`.
Default worker activation, whole M6 parity, and comparative performance
remain open. [Redacted receipt](../scripts/rust-integration/m6-5d9e558b-account-expiration.json)

Original Dispatcharr, Django, Celery and django-celery-beat authors and
contributors retain full credit.
