# Scanner-owned parented Refresh

Original Dispatcharr/Celery/Billiard/Kombu/Django/Redis/Channels authors retain code, algorithm, and licensing credit. This is normal inert library code, not an enabled Main consumer.

The trusted scanner assembly explicitly pins a parent UUID and up to four source IDs. The successful bounded parent metadata must list the child. The immediate child must match the parent/root/source, initial tuple args, no ETA/force/retry. This grants no authority to existing Refresh/Default/Index intake. Failed-parent/orphan, unlisted repeat-ID children, unknown shapes and other families remain unsupported before ACK. There is no automatic poller or durable capability recovery.

Actual roles: Original scanner executes on an explicit fixture-only parent queue, producing real default-queue children; native executes the two Refresh children with the existing real FileJob/immutable dispatcher; then unchanged Original consumes every Index/Series descendant on `celery`. No child is deleted or quarantined. The inherited Source-only harness introductory docstring is historical; these explicit roles and verified summary describe this native fixture.

The two functional cases compare exact Source event payload/order, index JSON, stable source fields, and typed null results/parent/children/TTLs through actual Original AsyncResult. Programme case has no mapped Channel: no ProgramData/guide claim. Three extra cases hold actual Redis replies after successful-parent read, CAS pop, and result commit. Caller abort, admission, and shutdown stay retained until release. They do not claim a logical two-second timeout or lost-reply matrix.

ACK scope: the failed first portable replay retains an actual Source Index reservation at `task_prerun` with `acks_late=false`. Original early ACK is an acceptance callback policy, not a universal physical-prerun ordering promise. The unchanged Source task separately blocked in real owned FIFO I/O proves eventual ACK during held handler execution before result. Public observers now retain instant reservation states as diagnostics. Native CAS/physical controls remain unchanged. Prior Source100/private150 successes remain historical point observations.

Reproduce with fresh output paths (no private source fallback):

```sh
python3 <here>/test_reconstruct.py --repo .
python3 <here>/reconstruct.py --repo . --output <source>
python3 <here>/test_build_peer.py --source <source>
python3 <here>/test_acceptance.py
python3 <here>/build_peer.py --source <source> --target <fresh-target> --output <build>
python3 <here>/run_pair.py --repo . --support-root scripts/rust-core-perf/support --native-binary <build>/dispatcharr_job_transport --reference <here>/source-children-4.json --output <fresh-evidence>
python3 <here>/ack-witness/run_pair.py --repo . --support-root scripts/rust-core-perf/support --output <fresh-ack-evidence>
```

Reconstruction is immutable committed772 plus sixteen exact packaged overlays, yielding782; five changed paths relative to historical qualified779. Support uses committed explicit files/hashes and five pinned compile-time goldens. No Main/Cargo/graph edits. Both runtime fixtures use pinned Original image; Rust fixture ELF is rebuilt from exact source. Whole checks and fresh portable150 passed; raw failures and old ELF remain retained.
