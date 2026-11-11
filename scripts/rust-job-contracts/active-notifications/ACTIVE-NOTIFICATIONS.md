# Active account expiration notifications

The pinned Original `check_account_expirations` task ran in a real prefork worker against an owned copied database, broker DB8, result DB9 and actual Channels Redis. Source2 passes 219 checks across 17 cases. A task-scoped fixed clock controls the timezone dependency; task/helper implementations, ORM, backend and Channels remain unchanged. Celery result timestamps use the real clock. Original Dispatcharr and its dependency authors retain credit.

The task selects active profiles with a non-null stored expiration belonging to active accounts. `exp <= now` produces the expired/high notice. The inclusive seven-day boundary produces warning/normal; remaining whole days are floored. At seven days plus one microsecond both notices are collected. Missing dates, inactive profiles and inactive accounts are excluded, while final stale cleanup still runs.

Each profile first commits an independent opposite-notice collector deletion, emits its dismissal, commits an update-or-create, then emits the notice. Final stale cleanup is another independent collector transaction. Upsert preserves existing source, creation time, expiry and user dismissals while replacing content/action fields and updating `updated_at`. A new notice has the model's system source and null expiry. Every upsert sends an event, including updates. Events contain the exact captured field set; created time uses Python ISO formatting.

Actual SQL faults at opposite deletion, upsert and final stale deletion demonstrate different committed prefixes. A Channels wrong-type error is caught after the SQL commit and still returns SUCCESS/null. A profile name of 255 characters causes a real varchar(255) title failure after the prior opposite deletion. This is an explicit unsupported native domain; no truncation.

The worker's process-local proxy manager maps are empty. Its global pre_delete receiver is captured; this does not establish effects in a separate HTTP process. Concurrent profile/key mutation, multiple eligible profile iteration order, same-process live proxy managers and native Python exception rendering remain unqualified.

The first native extension is an explicit inert constructor for zero or one eligible profile, profile name at most 237 characters, bounded year/date domain, 128 collector notices and 1024 dismissals, with four retained service admissions. Existing inactive constructors keep their old behavior. Unknown/multiple profiles refuse before ACK. This is a subset, not full family closure; Main/default consumers remain off.
