# Cached Redis restart differential

Original Dispatcharr authors retain application and algorithm credit. Redis and redis-py retain server/client credit. This fixture changes no production reconnect implementation.

A network-isolated, read-only container runs Redis under a tiny process supervisor. The container and loopback port6379 stay fixed while the actual Redis process stops and restarts without persistence. Redis run IDs and old-key loss prove that a real restart occurred. No PostgreSQL, provider/application startup or external network is used; the Original peer asserts Django apps were never initialized.

A persistent Python peer calls the actual pinned Original `RedisClient.get_client`, `acquire_task_lock` and `release_task_lock` helpers. A persistent native peer keeps its original TaskLocks and RedisTransport instances. Both execute healthy, unavailable and restarted-server operations. Source-held/native-held locks, returned errors/errno diagnostics, resource settlement and cached-object identity are recorded. Reconstructed native clients provide an independent positive control on the same server after testing cached instances at declared delays0/0.1/1seconds.

The runner distinguishes fixture completion from migration acceptance. A valid experiment that demonstrates unrecovered cached native connections has `execution_passed:true` and `migration_gate_passed:false`. It must not be summarized as a passed recovery gate. A three-attempt observation is not a long-term timing guarantee; implementation review determines whether a reconnect path exists.

```sh
source artifacts/rust/tooling/environment.sh
python3 scripts/rust-redis-restart-contracts/run.py \
  --output artifacts/rust/redis-cached-restart-next
```

Rust and tool closures, the actual native binary, immutable Original revision/image and cleanup evidence are bound in each output's `proof.json`. The example binary is mounted into the Original runtime for ABI compatibility; this is a component probe, not a rebuilt application/Compose deployment or a performance measurement.

The later [bounded recovery design](RECOVERY-DESIGN.md) and
[combined checkpoint](../rust-integration/verified-recovery-permission-components-1.json)
supersede the initial mismatch only for TaskLocks, main Coordinator and shared
RedisTransport. Other independent cached clients, first-operation parity, full
application recovery and performance remain open. The probe itself never edits
production code; the separately reviewed runtime change is explicit in those
manifests.
