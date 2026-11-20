# Daily account expiration in the selected worker

Original Dispatcharr, Celery and Django Celery Beat authors retain full credit.

On application `82414407934a2ac19f01949df302e9715f667261`, both Compose lanes ran the stock daily task once. Each completed with a null result, updated the active warning in place and created the expired notice. The obsolete warning and stale inactive-account notice were deleted and dismissed. Both notification updates and both dismissals reached the actual Channels receiver. The three profile rows and unrelated notice were unchanged.

Both queues drained, Rust Main exited 0 and all five tracked container and host generations retired. This is a scoped result for the explicitly selected worker and scheduler. Full M6 and performance remain open.

[Redacted receipt](../scripts/rust-job-contracts/account-expiration/verified-82414407-daily-pair-1.json)
