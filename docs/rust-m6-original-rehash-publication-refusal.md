# Original provider rehash with a definite broker refusal

The test held the ordinary Celery worker and applied a temporary LPUSH
restriction to the owned Redis fixture. An authenticated hash-key PATCH
returned HTTP 500, with a 145-byte HTML body and two broker-recorded LPUSH
denials. PostgreSQL already contained the new override. The publication
journal and queue contained no rehash job, and the Redis token was absent.

After restoring and checking the broker permission, the same-value PATCH
returned HTTP 200 and added no work. The original streams and unrelated
provider rows matched their starting state. A distinct worker generation
came up with an empty queue. Both owned containers and the observer
retired exactly.

The first run had already observed the HTTP 500 and both denials. Its
controller exited 1 because Redis rendered restored command permissions
with an extra explicit LPUSH allowance. That attempt retired exactly. The
corrected independent run checked restored permission with Redis ACL DRYRUN
and completed the retry and retirement assertions.

The Native error wire and accepted-publication response-loss cases remain
open. Original Dispatcharr, Django, Celery and Redis authors retain full
credit.

[Redacted evidence](../scripts/rust-integration/m6-original-rehash-publication-refusal.json)
