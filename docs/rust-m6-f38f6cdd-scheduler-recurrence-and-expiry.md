# F38 scheduler install, recurrence and expiry

The exact F38 ordinary AMD64 package passed the saved-default scheduler
install and tick rows. The installer preserved five known defaults and
reinstalled the missing `fetch-channel-statuses` row. The tick observed
two due arms at the configured 3,600-second cadence. Both canonical rows
passed their cleanup and independent evaluator.

The signed recurrence pair passed on Original and Native. Each backend
ran the configured task twice, returned `1` both times, linked two artwork
records and replaced the booking in its existing slot. Other owned
bookings stayed unchanged, and no future media was dispatched.

Original and Native agreed on all three backend expiry cases. Expired new
work returned an expired result before acknowledgement. A fresh delivery
to the same worker returned revoked before acknowledgement. An expired
delivery with an existing successful record preserved that record. Both
fixtures verified their owned teardown.

The scheduler sub-dispatch exited 2 for its deliberately incomplete matrix.
The enclosing scheduler group, both physical legs, both proof phases and
the independent replay exited 0. The replay reproduced the original
inner and outer group proofs byte for byte.

| Evidence | SHA-256 |
| --- | --- |
| Canonical scheduler install | `1ff85351ce7e916110cd7a9c059e12221a6668f160643c2b0a7686d2c239b9e1` |
| Canonical scheduler tick | `fc0b5cee7bee70709202aaae91e60afa74b3889fdafbb9db983888fb9ea2ecd5` |
| Signed recurrence pair | `8ba9016caf4dfcd6a710df7d5f490c8c54a503395cde42deabe57762542fdee3` |
| Backend cleanup and expiry pair | `c27f9cb2ef2b997b8be889dedf61214518b71f0160a1ff3b53b37ae5198b1f85` |
| Independent group replay | `777ec7d760bd4f7500db7779821f0468ae0a297e43196d1d207867a2712bd182` |

These verified current receipts satisfy the four predecessor edges for
a separate Stats run on application
`f38f6cdd09ad214d3d3ff18b0a46c286637fc9d1` and published merge
`7927b2d4765f82b650d958fd15d0b70200a6b72c`. Stats, the scanner,
account-warning expiration, Version503, Version200 and scheduler normal
stop still require their own evidence. The script-only adapter preserves
their canonical dependency order and records no new execution for the
four imported receipts. This work does not measure performance or close
all of M6.

The original Dispatcharr, Django, Celery and dependency authors retain
full authorship and license credit.
