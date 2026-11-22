# Original provider rehash with a held worker

This Original-only control accompanies the scoped provider rehash on
`55c895bf`. The fixture gracefully retired one Celery worker while the
authenticated web role stayed live. A hash-key-only PATCH on the owned
inactive STD provider returned HTTP 200. PostgreSQL already showed the
new override while Redis held one matching rehash frame and lock.
Repeating the same-value PATCH also returned HTTP 200 and added no frame.
The checkpoint found one ready or reserved job and no registered Celery
worker.

A distinct Celery generation consumed the recorded task and returned
`Successfully rehashed 2 streams`. The expected merge and channel links
were verified, the queue drained, and the lock cleared. The same
observer and both owned container roles retired exactly. The controller
and independent reducer exited 0.

The preceding controller exited 1 because its role assertion omitted the
registered Redis observer and short-circuited before sampling the aggregate
queue count. Its retained journal recorded one rehash publication and no
unrelated publications. The replacement worker had yet to start. Exact
cleanup passed.

This control measures acknowledged publication and an unchanged-value
request. The definite publication refusal and accepted-but-unsettled
reply remain unmeasured, as do their Native 501 and 503 responses. The
Original Dispatcharr, Django and Celery authors retain full credit.

[Redacted receipt](../scripts/rust-integration/m6-55c895-original-rehash-held-worker.json)
