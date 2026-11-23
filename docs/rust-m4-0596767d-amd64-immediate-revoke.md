# Signed immediate DVR Series revoke on AMD64

The `0596767d` ordinary AMD64 package and Original `1241b3f7` each handled one signed administrator POST to `/api/channels/series-rules/bulk-remove/`. Both returned HTTP 200 with the same 28-byte JSON response, published one nonterminating, no-reply Redis revoke for the selected recording task, and emitted one recordings-refreshed event. PostgreSQL showed exactly one deletion, an unchanged sibling, and zero same-name Beat rows before the request, after the request, and after app exit.

The Redis observer also confirmed a wire difference. Original sent integer `expires: 0` and `signal: null`. Native sent a floating expiry timestamp and `signal: "SIGTERM"`. Both sent `terminate: false`. The Original fixture ran uWSGI without a Celery worker, so the selected task had no result. The Native fixture ran its ordinary AIO control subscriber and persisted `REVOKED` for the selected task. Each sibling’s result stayed absent.

Both applications exited cleanly and the owner independently verified retirement of two containers and one network per arm, with no remaining volume. The prior expiry-observer failure and the corrected same-package result are separately retained. This replay covered successful immediate publication. Proven early nonpublication, ambiguous writer outcomes, and worker playback remain open.

Original Dispatcharr, Django, Celery, and Kombu contributors retain full credit. [Exact evidence receipt](../scripts/rust-integration/m4-0596767d-amd64-immediate-revoke.json)
