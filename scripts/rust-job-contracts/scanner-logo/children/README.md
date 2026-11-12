# Actual Original scanner child consumption

Original Dispatcharr, Django, Celery, Kombu, Redis, and Channels authors retain application/algorithm credit and licensing.

The unchanged pinned Original scanner and all descendants execute on the real default `celery` queue in one concurrency-1 prefork worker. No task is routed away or deleted. The parent scanner clock alone is fixed; child task wall clocks remain real. Six owned synthetic cases cover empty/programme EPG, inactive/empty/one-stream M3U, and combined empty files. No media URI is played or provider contacted.

Run from the repository with `python3 <this-directory>/run_pair.py --repo . --support-root scripts/rust-core-perf/support --output <fresh-output>`. This creates and removes a disposable copied database and internal-only Compose project; broker/result DBs are 8/9. Tools never capture mutable Rust.

The final dataset retains task-main-connection SQL command order/errors (M3U worker-thread SQL is not observed; final Stream rows are captured), task attributes, publication/received/prerun/postrun order, exact lineage, result metadata/raw bytes/TTLs, per-task cache bytes, final rows/events/files and actual own-task early ACK before prerun. Other prefetched task reservations may remain during a task; zero global unacked count is only required at final drain.

Each EPG parent yields two Refresh, three Index and two Series0 descendants. Refresh/Index normal results are null; Series0 returns its observed dict. No Channel is mapped in these cases: the nonempty EPG creates one EPGData row/index and no ProgramData rows. M3U inactive groups returns `[inactive-message, null]`; empty groups returns `[[], {"Default Group": {}}]`. Empty single refresh succeeds with a failure-description string and error status. The one-stream groups result contains actual parsed fields; single refresh returns `Dispatched jobs complete.` and writes one Stream. Group cache JSON is observed after the groups task and single-task effects separately.

These are Source-only contracts. Busy/deferral/hardloss/repeated-ID child behavior and native consumer/resource parity require independent gates. No old default consumer gains parented authority from this capture.

Failed attempts 1/2 and successful narrower attempt3 remain at their original artifact paths; final4 is authoritative for the 100 assertions.
