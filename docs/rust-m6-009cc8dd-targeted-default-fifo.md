# M6 targeted default queue pause and one-off DVR recording

The paired AMD64 Docker run used pinned Original and Rust `009cc8dd`. In each fresh fixture, the authenticated Celery caller paused the named default receiver through its existing broker control channel. The DVR receiver stayed active. The signed booking used a 75-second lead and the same 61-second window. Each live default receiver first settled one real booking-artwork job. After the receipt confirmed an empty default queue and reservation indexes, two natural 20-second scanner jobs reached the paused queue before recording started. Their exact bytes and FIFO order remained intact through natural DVR completion, recorder retirement, and terminal schedule-row deletion.

The Original acknowledgement came from the stock worker's active-queues response. Rust reported its paused default receiver through the existing readiness route. The second real control call resumed each default receiver, whose two tasks finished in FIFO order and produced matching successful Redis DB2 results.

| Verified observation | Original | Rust |
|---|---:|---:|
| Signed booking, active and terminal HTTP status | 201 / 200 / 200 | 201 / 200 / 200 |
| Actual booking artwork jobs completed before pause | 1 | 1 |
| Actual targeted control publications | 2 | 2 |
| Natural default jobs held without a claim | 2 | 2 |
| Ordered default tasks completed after resume | 2 | 2 |
| Owned programme-index descendants completed after resume | 1 | 1 |
| Scheduled recording window | 61 s | 61 s |
| Remaining enabled one-off task and clocked row | 0 | 0 |
| Mapped video/audio decoder to EOF exit | 0 | 0 |
| Separate strict decoder exit | 183 | 183 |
| Retired containers and host generations | 2 | 3 |

After resume, the retained Source and Rust journals recorded 1 and 1 scoped programme-index descendants. Each observed descendant addressed the copied EPG source, carried its scanner UUID as both parent and root, and reached a matching DB2 terminal result. This count is separately recorded from the two siblings held during the recording.

Both arms kept the original schedule display identity separate from the natural execution UUID. The UUID joined the DB1 publication, the signed completed recording, and a successful DB2 worker result. The exact PeriodicTask and its unused ClockedSchedule were absent after the recording, while the Recording row remained completed and remuxed. The optional strict decoder result is recorded independently of the required whole-file EOF check.

All five owner phases returned zero. The scoped container, network, and volume inventories were empty and the original process generations were absent. The package digest and scoped receipts are in the accompanying evidence. This covers the targeted default FIFO and one-off terminal SQL comparison. The formal M6 controller and performance gates remain open.

Original Dispatcharr, Celery, Django, and dependency authors retain full credit.
