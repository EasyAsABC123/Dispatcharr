# AMD64 held default queue and one-off recording on 0596767d

Pinned Original and exact Rust 0596767d each accepted the same signed booking with a 75-second lead and a 61-second recording window. Each default receiver completed the booking-artwork job before the targeted authenticated pause. The DVR receiver stayed active. Two natural scanner siblings held the same Redis bytes and FIFO order before recording began and after it completed. There were zero default claims during the hold.

Original reported the pause and resume through Celery active queues. Rust used its named queue-control destination and existing readiness route. Each receiver resumed the same two scanner jobs in order and recorded two matching successful Redis results. One programme-index child from the first scanner also completed in each arm. The queues drained.

| Verified observation | Original | Rust |
| --- | ---: | ---: |
| Signed booking, active and terminal status | 201 / 200 / 200 | 201 / 200 / 200 |
| Held scanner siblings | 2 | 2 |
| Natural DVR publications with a matching successful result | 1 | 1 |
| Remaining one-off PeriodicTask and unused ClockedSchedule rows | 0 | 0 |
| Recorded MKV duration | 46.33 s | 46.33 s |
| Video and audio frames | 1,155 / 1,920 | 1,155 / 1,920 |
| Declared mapped video and audio decode to EOF exit | 0 | 0 |
| Separate strict decoder exit | 183 | 183 |
| Exact retired containers and host generations | 2 | 3 |

The natural execution UUID matched the broker publication and the stored successful result. The schedule name and completed, remuxed recording row persisted while read-only SQL verified retirement of its one-off scheduler rows. Both MKVs contained MPEG-2 video and mono MP2 audio with equal stream frame counts and distinct whole-file hashes. The strict decoder outcomes remain in the evidence separately from the declared EOF check.

All five owner phases exited 0. Original Web, Beat, Celery and DVR roles and Rust Main exited cleanly. Both scoped container, network and volume inventories were empty. The scoped test leaves the ordered M6 predecessor gates, full M6 acceptance and performance open. Original Dispatcharr, Celery, Django, and dependency authors retain full credit and licenses. [Sanitized evidence](../scripts/rust-integration/m6-0596767d-amd64-fifo-oneoff.json)
